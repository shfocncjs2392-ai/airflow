from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import logging
import json
import requests

API_URL = "http://host.docker.internal:8000/predict"

def _create_dummy_data(**kwargs):
    users = [
        {"user_id" : "C001", "income" : 5000, "loan_amt" : 2000},
        {"user_id" : "C002", "income" : 4000, "loan_amt" : 5000},
        {"user_id" : "C003", "income" : 8000, "loan_amt" : 1000},
    ]
    return users

def _api_service_call(**kwargs):
    ti = kwargs['ti']
    # 4-1에서 만든 [{user_id, income, loan_amt}, ...] 데이터
    users_data = ti.xcom_pull(task_ids='task_create_dummy_data')
    
    try:
        res = requests.post(API_URL, json=users_data)
        res.raise_for_status()
        
        predictions = res.json()  # API는 보통 [{"credit_score": 80, "grade": "B"}, ...] 만 줌
        
        # 원래 입력 데이터(users_data)와 API 결과(predictions)를 인덱스 기준으로 합침
        combined_result = []
        for i in range(len(users_data)):
            merged = {**users_data[i], **predictions[i]}
            combined_result.append(merged)
            
        logging.info(f"데이터 병합 완료: {combined_result}")
        return combined_result

    except Exception as e:
        logging.error(f"API 호출 또는 병합 실패 : {e}")
        raise

def _load_users_credict(**kwargs):
    ti = kwargs['ti']
    users_credict_data = ti.xcom_pull(task_ids='task_api_service_call')
    
    if not users_credict_data:
        logging.error("신용 평가 결과 없음")
        raise ValueError("No data found in XCom") # 명확한 에러 타입 명시

    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    
    # conn을 try 밖에서 선언하거나, Hook의 기능을 최대한 활용
    try:
        conn = mysql_hook.get_conn()
        cursor = conn.cursor()
        
        # 1. 테이블 생성
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS customers (
                user_id VARCHAR(50) PRIMARY KEY,
                income INT DEFAULT NULL,
                loan_amt INT DEFAULT NULL,
                credit_score INT DEFAULT NULL,
                grade VARCHAR(50) DEFAULT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        # 2. 데이터 준비
        insert_sql = """
            INSERT INTO customers (user_id, income, loan_amt, credit_score, grade)
            VALUES (%s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            income = VALUES(income),
            loan_amt = VALUES(loan_amt),
            credit_score = VALUES(credit_score),
            grade = VALUES(grade)
        """
        params = [
            (
                data.get('user_id'), 
                data.get('income'), 
                data.get('loan_amt'), 
                data.get('credit_score'), 
                data.get('grade')
            )
            for data in users_credict_data
        ]

        # 3. 실행 및 커밋
        cursor.executemany(insert_sql, params)
        conn.commit()
        print(f"Loaded {len(params)} rows to MySQL.")

    except Exception as e:
        # conn이 성공적으로 생성되었을 때만 rollback 실행
        if 'conn' in locals() and conn:
            conn.rollback()
        logging.error(f"Error occurred: {e}")
        raise e
    finally:
        # cursor와 conn이 존재하는지 확인 후 닫기
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()


with DAG(
    default_args= {'owner' : 'YONG'},
    dag_id="07_msa_api_server_used",
    description = "MSA 상에 특정 서비스(AI 서빙 컨셉)를 호출하여 신용평가 수행하는 스케줄링",
    start_date=datetime(2026, 2, 25),
    schedule_interval='@daily',
    catchup=False,
    tags=['msa', 'fastapi']
) as dag:
    
    # 4. Task 정의
    # 4-1. 더미 데이터 준비 -> 추후 고객 정보 저장 -> 추후 s3 업로드
    task_create_dummy_data = PythonOperator(
        task_id = "task_create_dummy_data",
        python_callable = _create_dummy_data
    )
    
    # 4-2. API 호출(AI 서비스 활용) -> 신용평가획득
    task_api_service_call = PythonOperator(
        task_id = "task_api_service_call",
        python_callable = _api_service_call
    )
    
    # 4-3. 결과저장 -> 추후 고객 정보 업데이트
    task_load_users_credict = PythonOperator(
        task_id = "task_load_users_credict",
        python_callable = _load_users_credict
    )

    #5. 의존성
    task_create_dummy_data >> task_api_service_call >> task_load_users_credict