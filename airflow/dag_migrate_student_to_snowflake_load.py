from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from load_student_data_to_snowflake import ingest_student_data_to_snowflake
from airflow.operators.empty import EmptyOperator

# Define the default arguments for the DAG
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 22),
    'email_on_failure': False,
    'email_on_retry': False,
}

# Define the DAG
with DAG(
    dag_id='dag_migrate_student_to_snowflake_load',
    default_args=default_args,
    description='This DAG reads student data from JSON files and writes to a Snowflake table',
    schedule_interval='0 3 * * *',  # CRON expression for 3:00 AM daily
    tags=['JSON', 'SNOWFLAKE', 'STUDENT_DATA'],
    catchup=False,
) as dag:
    # Define the start and end tasks
    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    # Define the task to execute the Python function
    task_ingest_student_data_to_snowflake = PythonOperator(
        task_id='ingest_student_data_to_snowflake',
        python_callable=ingest_student_data_to_snowflake,
    )

    # Define the dependency between tasks
    start >> task_ingest_student_data_to_snowflake >> end
