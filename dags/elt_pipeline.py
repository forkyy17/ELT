from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

# Функция для запуска скрипта загрузки CSV
def load_csv_to_clickhouse():
    script_path = os.path.join(os.path.dirname(__file__), '../scripts/load_csv_to_ch.py')
    sys.path.append(os.path.dirname(script_path))
    import load_csv_to_ch
    load_csv_to_ch.main()

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='elt_pipeline',
    default_args=default_args,
    schedule_interval=timedelta(minutes=1), 
    catchup=False,
    description='ELT pipeline: CSV -> ClickHouse -> dbt',
) as dag:

    load_csv = PythonOperator(
        task_id='load_csv_to_clickhouse',
        python_callable=load_csv_to_clickhouse,
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command=(
            'export DBT_PROFILES_DIR=/opt/airflow/dbt/profiles && '
            'cd /opt/airflow/dbt && '
            'dbt run'
        ),
    )

    load_csv >> dbt_run


