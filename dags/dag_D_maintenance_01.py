import os
import datetime as dt
import shutil

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Configurations

LOG_FOLDER = '/home/ubuntu/MovieReco/logs' # A changer si besoin
# LOG_FOLDER = '/usr/local/airflow/logs' # A changer si besoin
DAYS_TO_RETAIN = 1 # Pareil ici


def clean_old_logs(*args, **kwargs):
    """
    Supprime les anciens logs.
    """
    now = dt.datetime.now()
    cutoff_date = now - dt.timedelta(days=DAYS_TO_RETAIN)

    for subdir, dirs, files in os.walk(LOG_FOLDER):
        for dir_ in dirs:
            dir_path = os.path.join(subdir, dir_)
            dir_date = dt.datetime.strptime(dir_, '%Y-%m-%d')

            if dir_date < cutoff_date:
                try:
                    shutil.rmtree(dir_path)
                    print(f"Removed old logs in directory: {dir_path}")
                except Exception as e:
                    print(f"Error removing directory {dir_path}: {e}")

    print(f"Cleaned logs older than {DAYS_TO_RETAIN} days.")


# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=5),
}

dag = DAG(
    'clean_old_logs',
    default_args=default_args,
    description='A DAG to remove old Airflow logs',
    schedule_interval=dt.timedelta(days=1),  # run every day
    start_date=days_ago(1),
    catchup=False
)

clean_logs_task = PythonOperator(
    task_id='clean_logs',
    python_callable=clean_old_logs,
    provide_context=True,
    dag=dag
)