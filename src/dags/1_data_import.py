from airflow import DAG
import datetime
from airflow.operators.python import PythonOperator
import vertica_python

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
    interested_datetime = context['ds']
    interested_batch_number = int(interested_datetime.split('-')[-1])
    sql_text = f"COPY temp_name_STAGING.transactions FROM 's3://final-project/transactions_batch_{interested_batch_number}.csv';"
    conn_info = { # потом подставлю значения
        'host': 'host_name', 
        'port': 'port_number', 
        'user': 'user_name', 
        'password': 'user_password', 
        'database': 'db_name'
    }
    conn = vertica_python.connect(**conn_info)
    cur = conn.cursor()
    
    if interested_batch_number == 1:
        cur.execute("""
            COPY temp_name_STAGING.currencies
            FROM 's3://final-project/currencies_history.csv';
        """)
        cur.execute(sql_text)
        conn.commit()
        conn.close()
    else:
        cur.execute(sql_text)
        conn.commit()
        conn.close()

s3_to_staging_transfer = PythonOperator (
    task_id='s3_to_staging_transfer',
    python_callable = s3_to_staging_task,
    dag=dag
)