from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.datasets import Dataset
import pandas as pd

# 데이터셋 참조
PROCESSED_DATA_CSV = Dataset('/opt/airflow/dags/data/preprocessing_data.csv')

def _load():
    df = pd.read_csv(PROCESSED_DATA_CSV.uri)
    mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
    conn = mysql_hook.get_conn()
    
    try:
        with conn.cursor() as cursor:
            insert_sql = """
                INSERT INTO sensor_readings (sensor_id, timestamp, temperature_c, temperature_f)
                VALUES (%s, %s, %s, %s)
            """
            params = [
                (data['sensor_id'], data['timestamp'], data['temperature_c'], data['temperature_f'])
                for _, data in df.iterrows()
            ]
            cursor.executemany(insert_sql, params)
            conn.commit()
            print(f"Loaded {len(params)} rows to MySQL.")
    except Exception as e:
        conn.rollback()
        raise e
    finally:
        conn.close()

with DAG(
    default_args= {'owner' : 'YONG'},
    dag_id="06_multi_dag_step3_load",
    start_date=datetime(2026, 2, 25),
    schedule=[PROCESSED_DATA_CSV], # CSV 데이터셋이 업데이트되면 자동 실행
    catchup=False,
    tags=['multi_dag', 'load']
) as dag:

    task_create_table = SQLExecuteQueryOperator(
        task_id="create_table",
        conn_id="mysql_default",
        sql='''
            CREATE TABLE IF NOT EXISTS sensor_readings (
                id INT AUTO_INCREMENT PRIMARY KEY,
                sensor_id VARCHAR(50),
                timestamp DATETIME,
                temperature_c FLOAT,
                temperature_f FLOAT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        '''
    )

    task_load = PythonOperator(
        task_id="load_task",
        python_callable=_load
    )

    task_create_table >> task_load