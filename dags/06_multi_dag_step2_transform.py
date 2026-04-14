from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.datasets import Dataset
import pandas as pd
import json

# 데이터셋 참조
RAW_DATA_JSON = Dataset('/opt/airflow/dags/data/sensor_data.json')
PROCESSED_DATA_CSV = Dataset('/opt/airflow/dags/data/preprocessing_data.csv')

def _transform():
    # JSON 로드
    with open(RAW_DATA_JSON.uri, 'r') as f:
        data = json.load(f)
    
    df = pd.DataFrame(data)
    
    # 전처리: 화씨 계산
    df['temperature_f'] = (df['temperature'] * 9/5) + 32
    df.rename(columns={'temperature': 'temperature_c'}, inplace=True)
    
    # CSV 저장
    df.to_csv(PROCESSED_DATA_CSV.uri, index=False)
    print(f"Transformed data to {PROCESSED_DATA_CSV.uri}")

with DAG(
    default_args= {'owner' : 'YONG'},
    dag_id="06_multi_dag_step2_transform",
    start_date=datetime(2026, 2, 25),
    schedule=[RAW_DATA_JSON], # JSON 데이터셋이 업데이트되면 자동 실행
    catchup=False,
    tags=['multi_dag', 'transform']
) as dag:
    
    task_transform = PythonOperator(
        task_id="transform_task",
        python_callable=_transform,
        outlets=[PROCESSED_DATA_CSV] # 이 파일이 생성됨을 알림
    )