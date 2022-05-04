# Airflow 實作 - 漫畫追蹤程式
<div>參考https://github.com/leemengtaiwan/airflow-tutorials</div>
<div>文章請詳見<a href="https://leemeng.tw/a-story-about-airflow-and-data-engineering-using-how-to-use-python-to-catch-up-with-latest-comics-as-an-example.html">此篇</a></div>

## Create the environment

```shell
# 創造 conda 環境
conda create -n airflow-tutorials python=3.6 -y
# 在此環境下安裝 Airflow 以及支援 Slack 功能的額外函式庫
source activate airflow-tutorials
pip install "apache-airflow[crypto, slack]"
# 設定專用路徑以讓 Airflow 之後知道要在哪找檔案、存 log
export AIRFLOW_HOME="$(pwd)"
# 初始化 Airflow Metadata DB。此 DB 被用來記錄所有工作流程的執行狀況
airflow db init
```

## open webserver

```shell
airflow webserver -p 8080
```
,then open the webserver by
```python
localhost:8080
```
If the web server ask to log in, you should create a user first
```shell
airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
```

## open airflow scheduler

Enter
```shell
airflow scheduler
```
