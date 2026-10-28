from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
import vertica_python
from airflow.hooks.base import BaseHook
# Версия 3
import boto3
import os
import pandas as pd
import tempfile
import io
# Версия 3 - конец блока

default_args = {
    'owner': 'dlgvzvs',
    'retries': 3
}

dag = DAG(
    's3_to_staging_dag',
    default_args = default_args,
    start_date = datetime(2022, 10, 1, 0, 0, 0),
    end_date = datetime(2022, 11, 1, 0, 0, 0),
    schedule_interval = '@daily',
    catchup = True
)

def s3_to_staging_task(**context):
    bucket_name = 'final-project'
    temp_path = 'C:/Users/sdolg/OneDrive/Документы/' # сейчас указана локлаьная папка, но куда лучше? в hdfs? или куда? НУЖНО УТОЧНИТЬ МЕСТО!!!
    airflow_conn = BaseHook.get_connection('vertica_conn')
    ## По-прежнему не удаётся получить данные для соединения с Vertica из-за ошибки 503 (28.10.2026 - ситуация не изменилась)
    conn_info = {
        'host': airflow_conn.host,
        'port': airflow_conn.port,
        'user': airflow_conn.login,
        'password': airflow_conn.password,
        'database': airflow_conn.schema,
    }
    conn = vertica_python.connect(**conn_info)
    interested_datetime = context['ds']
    cur = conn.cursor()
    
    ## Версия 3 - начало блока
    s3 = boto3.client(
        's3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id='YCAJEiyNFq4wiOe_eMCMCXmQP',
        aws_secret_access_key='YCP1e96y4QI8OmcB4Eaf4q0nMHwhmtvGbDTgBeqS'
    )

    for i in range(1, 11):
        obj = s3.get_object(Bucket=bucket_name, Key=f'transactions_batch_{i}.csv')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        df['transaction_dt'] = pd.to_datetime(df['transaction_dt'])
        filtered_df = df[df['transaction_dt'].dt.strftime('%Y-%m-%d') == interested_datetime]
        local_file = os.path.join(tempfile.gettempdir(), f'filtered_batch_{i}.csv')
        filtered_df.to_csv(local_file, index=False)
        cur.execute (f"""
            COPY temp_name_STAGING.transactions
            FROM '{local_file}';
        """)
        
    if interested_datetime == '2022-10-01':
        obj = s3.get_object(Bucket=bucket_name, Key=f'currencies_history.csv')
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        df['date_update'] = pd.to_datetime(df['date_update'])
        filtered_df = df[df['date_update'].dt.strftime('%Y-%m-%d') == interested_datetime]
        local_file = os.path.join(tempfile.gettempdir(), 'filtered_currencies.csv')
        filtered_df.to_csv(local_file, index=False)
        cur.execute(f""" 
            COPY temp_name_STAGING.currencies 
            FROM '{local_file}';
            """)
    conn.commit()
    conn.close()
    ## Версия 3 - конец блока

s3_to_staging_transfer = PythonOperator (
    task_id='s3_to_staging_transfer',
    python_callable = s3_to_staging_task,
    dag=dag
)