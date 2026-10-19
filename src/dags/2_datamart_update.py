from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime
import vertica_python

default_args = {
    'owner': 'dlgvzvs',
    'retries': 3
}

dag = DAG(
    'staging_to_dwh_dag',
    default_args = default_args,
    start_date = datetime(2022, 10, 1, 0, 0, 0),
    end_date = datetime(2022, 11, 1, 0, 0, 0),
    schedule_interval = '@daily',
    catchup = True
)

def staging_to_dwh_task(**context):
    # 1. Получить дату
    interested_datetime = context['ds']
    # 2. Прочитать файл SQL
    sql_text = open("/opt/airflow/src/sql/staging_to_dwh_sql.sql", "r").read()
    # 3. Заменить {{ ds }} на дату
    sql_text = sql_text.replace('{{ ds }}', interested_datetime)
    # 4. Подключиться к Vertica (conn_info с хостом, портом, логином)
    conn_info = { # потом подставлю значения
        'host': 'host_name', 
        'port': 'port_number', 
        'user': 'user_name', 
        'password': 'user_password', 
        'database': 'db_name'
    }
    # 5. Выполнить SQL
    conn = vertica_python.connect(**conn_info)
    cur = conn.cursor()
    cur.execute(sql_text)
    # 6. Закрыть
    conn.commit()
    conn.close()

staging_to_dwh_transfer = PythonOperator (
    task_id='staging_to_dwh_transfer',
    python_callable = staging_to_dwh_task,
    dag=dag
)