import os
import re
import logging
import shutil
from dateutil.parser import parse
from datetime import timedelta, datetime, timezone

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

logs_path = None
retention_days = None
cron_schedule = None

# Function to extract datetime from folder name (generic)
def extract_datetime(folder_name):
    # Pattern: Capture anything before the datetime/date followed by YYYY-MM-DD(T[HH:MM:SS[.fff][+-]ZZ])?
    match = re.search(r'(.*?)(?P<datetime>\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(.\d+)?([+-])\d{2}:\d{2})?)', folder_name)

    if match:
        datetime_str = match.group('datetime')
        if datetime_str:
            try:
                return parse(datetime_str).astimezone(timezone.utc)
            except ValueError:
                logging.warning(f"Failed to parse datetime from folder name: {folder_name}")
                return None
    else:
        return None


def apply_retention():
    logging.basicConfig(level=logging.INFO)

    # Read logs path and retention days from Airflow variables
    try:
        global logs_path, cron_schedule, retention_days
        logs_path = Variable.get('logs_path', default_var='/opt/airflow/logs')
        retention_days = int(Variable.get('retention_days', default_var=180))
        cron_schedule = Variable.get('retention_cron_schedule', default_var='0 */8 * * *')
    except ValueError:
        logging.error("Error retrieving variables: logs_path, retention_days or retention_cron_schedule")
        return

    # Calculate retention threshold datetime
    retention_threshold = datetime.now(timezone.utc) - timedelta(days=retention_days)

    logging.info(f"Logs path: {logs_path}")
    logging.info(f"Retention days: {retention_days}")
    logging.info(f"Retention threshold: {retention_threshold}")

    for dir in os.listdir(logs_path):
        if dir == 'lost+found':
            continue
        dir_path = os.path.join(logs_path, dir)

        for subdir in os.listdir(dir_path):
            dir_datetime = extract_datetime(subdir)
            subdir_path = os.path.join(dir_path, subdir)
            if dir_datetime and dir_datetime < retention_threshold:
                logging.info(f"Removing directory: {subdir_path}")
                shutil.rmtree(subdir_path)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id='retention_policy',
        default_args=default_args,
        schedule_interval=cron_schedule,  # Run manually or through a trigger
) as dag:

    # Define the Python task to call the retention function
    apply_retention_task = PythonOperator(
        task_id='apply_retention',
        provide_context=True,
        python_callable=apply_retention,
        dag=dag,
    )
