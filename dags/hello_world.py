import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Define the function to print "Hello, World!"
def print_hello_world():
    print("Hello, World!")

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple DAG that prints "Hello, World!" every 5 minutes',
    schedule_interval='*/5 * * * *',
    start_date=datetime.datetime(2023, 1, 1),
    catchup=False,
)

# Define the task
hello_world_task = PythonOperator(
    task_id='print_hello_world',
    python_callable=print_hello_world,
    dag=dag,
)

# Set the task dependencies (if any)
hello_world_task
