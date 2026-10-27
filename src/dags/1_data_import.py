from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
import vertica_python
from airflow.hooks.base import BaseHook

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
    ## Версия 2
    bucket_name = 'final-project'
    source_path = f's3://{bucket_name}/'
    airflow_conn = BaseHook.get_connection('vertica_conn')
    ## По-прежнему не удаётся получить данные для соединения с Vertica из-за ошибки 503
    conn_info = {
        'host': airflow_conn.host,
        'port': airflow_conn.port,
        'user': airflow_conn.login,
        'password': airflow_conn.password,
        'database': airflow_conn.schema,
    }
    conn = vertica_python.connect(**conn_info)
    ## Конец нового блока (версия 2)
    interested_datetime = context['ds']
    cur = conn.cursor()
    
    if interested_datetime == '2022-10-01':
        cur.execute(f""" 
            COPY temp_name_STAGING.currencies 
            FROM '{source_path}currencies_history.csv'; 
            """)
    ## Версия 2 - блок 2
    for i in range(1, 11):
        cur.execute(f"""
        COPY temp_name_STAGING.transactions
        FROM '{source_path}transactions_batch_{i}.csv'
         WHERE transaction_dt >= '{interested_datetime}'::TIMESTAMP AND transaction_dt < ('{interested_datetime}'::TIMESTAMP + INTERVAL '1 day');
        """)
    ## Конец блока 2 (версия 2)
    conn.commit()
    conn.close()

s3_to_staging_transfer = PythonOperator (
    task_id='s3_to_staging_transfer',
    python_callable = s3_to_staging_task,
    dag=dag
)