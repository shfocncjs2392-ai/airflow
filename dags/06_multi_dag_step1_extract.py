from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import json
import random
import os

# 1. 데이터셋 정의 (트리거용)
RAW_DATA_JSON = Dataset('/opt/airflow/dags/data/sensor_data.json')
DATA_PATH = '/opt/airflow/dags/data'
os.makedirs(DATA_PATH, exist_ok=True)

def _extract():
    data = [
        {
            "sensor_id" : f"SENSOR_{i+1}", # 장비 ID
            "timestamp" : datetime.now().strftime("%Y-%m-%d %H:%M:%S"), # YYYY-MM-DD hh:mm:ss
            "temperature": round( random.uniform(20.0, 150.0), 2),
            "status" : "on", # "off"
        }
        for i in range(1, 11)
    ]
    file_path = RAW_DATA_JSON.uri
    with open(file_path, 'w') as f:
        json.dump(data, f)
    print(f"Extracted data to {file_path}")

with DAG(
    default_args= {'owner' : 'YONG'},
    dag_id="06_multi_dag_step1_extract",
    start_date=datetime(2026, 2, 25),
    schedule_interval='@daily',
    catchup=False,
    tags=['multi_dag', 'extract']
) as dag:
    
    task_extract = PythonOperator(
        task_id="extract_task",
        python_callable=_extract,
        outlets=[RAW_DATA_JSON] # 이 파일이 생성됨을 알림
    )