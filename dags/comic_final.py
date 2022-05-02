import os
import json
import time
from selenium import webdriver
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.slack_operator import SlackAPIPostOperator
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from airflow.operators.latest_only_operator import LatestOnlyOperator
default_args = {
    'owner': 'Feng Jim',
    'start_date': datetime(2022, 4, 29, 0, 0),
    'schedule_interval': '@daily',
    'retries': 5,
    'retry_delay': timedelta(seconds=30)
}

slack_hook_token = BaseHook.get_connection('fengjim_slack').password
comic_website_template = "https://www.cartoonmad.com/comic/{}.html"

def process_metadata(mode, **context):
    current_path = os.path.dirname(__file__)
    metadata_path = os.path.join(current_path, '../data/comic.json')
    if mode == 'read':
        with open(metadata_path,'r') as f:
            metadata = json.load(f)
            print("Read comic history:{}".format(metadata))
            return metadata
    elif mode == 'write':
        _, all_comic_information = context['task_instance'].xcom_pull(task_ids='check_comic_info')
        for comic_id, comic_vol in dict(all_comic_information).items():
            all_comic_information[comic_id]['prev_vol_num'] = comic_vol['latest_vol_num']

        with open(metadata_path,'w') as f:
            print("Recording the latest comic information")
            json.dump(all_comic_information,f,indent=2,ensure_ascii=False)

def check_comic_info(**context):
    metadata = context['task_instance'].xcom_pull(task_ids='get_read_history')
    driver = webdriver.Chrome()
    driver.get("https://www.cartoonmad.com/")
    print("Arrived the Home page")

    all_comic_info = metadata
    anyting_new = False
    for comic_id, comic_vol in dict(all_comic_info).items():
        comic_name = comic_vol['name']
        print("Searching comic {} list".format(comic_name))
        driver.get(comic_website_template.format(comic_id))

        # search the latest volume
        # the latest volume in the last one
        links = driver.find_elements_by_partial_link_text('第')
        latest_vol = [int(i) for i in links[-1].text.split() if i.isdigit()][0]
        pre_vol_num = comic_vol['prev_vol_num']

        all_comic_info[comic_id]['latest_vol_num'] = latest_vol
        all_comic_info[comic_id]['latest_vol_available'] = latest_vol > pre_vol_num
        if all_comic_info[comic_id]['latest_vol_available']:
            anything_new = True
            print("There is an new volume for {}(latest:{})".format(comic_name, latest_vol))

    if not anything_new:
        print("Nothing new")

    driver.quit()
    return anything_new, all_comic_info

def decide_what_to_do(**context):
    anything_new, all_comic_info = context['task_instance'].xcom_pull(task_ids='check_comic_info')

    print("跟紀錄比較，有沒有新連載？")
    if anything_new:
        return 'yes_generate_notification'
    else:
        return 'no_do_nothing'


def generate_message(**context):
    _, all_comic_info = context['task_instance'].xcom_pull(task_ids='check_comic_info')
    
    message = ""
    for comic_id, comic_vol in all_comic_info.items():
        if comic_vol['latest_vol_available']:
            name = comic_vol['name']
            latest_vol_num = comic_vol['latest_vol_num']
            prev_vol = comic_vol['prev_vol_num']
            message += "{} 最新一話:{}話（上次讀到{}話）\n".format(name,latest_vol_num,prev_vol)
            message += comic_website_template.format(comic_id) + "\n\n"
    file_dir = os.path.dirname(__file__)
    message_path = os.path.join(file_dir, "../data/message.txt")
    with open(message_path,"w") as f:
        f.write(message)

def get_message_text():
    file_dir = os.path.dirname(__file__)
    token_path = os.path.join(file_dir,"../data/message.txt")
    with open(token_path, 'r') as f:
        message = f.read()
    return message

with DAG('comic_app_v2', default_args=default_args) as dag:

    latest_only = LatestOnlyOperator(task_id='latest_only')

    get_read_history = PythonOperator(
        task_id='get_read_history',
        python_callable=process_metadata,
        op_args=['read'],
        provide_context = True
    )

    check_comic_info = PythonOperator(
        task_id='check_comic_info',
        python_callable=check_comic_info,
        provide_context=True
    )

    decide_what_to_do = BranchPythonOperator(
        task_id='new_comic_available',
        python_callable=decide_what_to_do,
        provide_context=True
    )

    update_read_history = PythonOperator(
        task_id='update_read_history',
        python_callable=process_metadata,
        op_args=['write'],
        provide_context=True
    )

    generate_notification = PythonOperator(
        task_id='yes_generate_notification',
        python_callable=generate_message,
        provide_context=True
    )

    send_notification = SlackWebhookOperator(
        task_id='send_notification',
        http_conn_id="fengjim_slack",
        webhook_token=slack_hook_token,
        message=get_message_text(),
        username='Comic_Elf'
    )

    do_nothing = DummyOperator(task_id='no_do_nothing')

    # define workflow
    latest_only >> get_read_history
    get_read_history >> check_comic_info >> decide_what_to_do
    decide_what_to_do >> generate_notification
    decide_what_to_do >> do_nothing
    generate_notification >> send_notification >> update_read_history
