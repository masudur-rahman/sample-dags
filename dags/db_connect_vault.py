import yaml
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the function to read the db-secrets.yaml file and extract the credentials
def read_db_secrets():
    with open('/vault/secrets/db-secrets.yaml', 'r') as file:
        secrets = yaml.safe_load(file)
    user = secrets['database']['user']
    password = secrets['database']['password']
    return user, password

# Define the function to connect to the database
def connect_to_db(**kwargs):
    ti = kwargs['ti']
    user, password = ti.xcom_pull(task_ids='read_secrets_task')

    conn = psycopg2.connect(
        dbname="activity",
        user=user,
        password=password,
        host="edb-postgres-vip.default.svc.cluster.local",
        port="5432"
    )

    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    db_version = cursor.fetchone()
    print(f"Connected to database. Version: {db_version}")

    cursor.close()
    conn.close()

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'db_access_dag',
    default_args=default_args,
    description='A simple DAG to read DB secrets and access the database',
    schedule_interval='@once',
)

# Define the tasks
read_secrets_task = PythonOperator(
    task_id='read_secrets_task',
    python_callable=read_db_secrets,
    dag=dag,
)

connect_db_task = PythonOperator(
    task_id='connect_db_task',
    python_callable=connect_to_db,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
read_secrets_task >> connect_db_task
