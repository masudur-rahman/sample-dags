from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.variable import Variable
import logging
import shutil
import os
from dateutil.parser import parse


# Function to extract datetime from folder name (generic)
def extract_datetime(folder_name):
    # Pattern: Capture anything before the datetime/date followed by YYYY-MM-DD(T[HH:MM:SS[.fff][+-]ZZ])?
    match = re.search(r'(.*?)(?P<datetime>\d{4}-\d{2}-\d{2}(T\d{2}:\d{2}:\d{2}(.\d+)?([+-])\d{2}:\d{2})?)', folder_name)

    if match:
        datetime_str = match.group('datetime')
        if datetime_str:
            try:
                return parse(datetime_str)
            except ValueError:
                logging.warning(f"Failed to parse datetime from folder name: {folder_name}")
                return None
    else:
        return None


def apply_retention():
    logging.basicConfig(level=logging.INFO)

    # Read logs path and retention days from Airflow variables
    try:
        logs_path = Variable.get('logs_path')
        retention_days = int(Variable.get('retention_days'))
    except ValueError:
        logging.error("Error retrieving variables: logs_path or retention_days")
        return

    # Calculate retention threshold datetime
    retention_threshold = datetime.now() - timedelta(days=retention_days)

    logging.info(f"Logs path: {logs_path}")
    logging.info(f"Retention days: {retention_days}")
    logging.info(f"Retention threshold: {retention_threshold}")

    for dir in os.listdir(logs_path):
        if dir == 'lost + found':
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
    'start_date': '{{ var.value.start_date }}',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
        dag_id='retention_dag',
        default_args=default_args,
        schedule_interval=None,  # Run manually or through a trigger
) as dag:

    # Define the Python task to call the retention function
    apply_retention_task = PythonOperator(
        task_id='apply_retention',
        provide_context=True,
        python_callable=apply_retention,
        dag=dag,
    )
