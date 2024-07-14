from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.cloud import bigquery
import pandas as pd
from airflow.operators.python_operator import PythonOperator

def ensure_dataset_exists(client, dataset_id):
    try:
        client.get_dataset(dataset_id)
        print(f"Dataset {dataset_id} already exists.")
    except Exception:
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        client.create_dataset(dataset)
        print(f"Created dataset {dataset_id}")

def upsert_data(**kwargs):
    print("======== this has been called ======")
    hook = BigQueryHook(bigquery_conn_id='test_big')
    client = hook.get_client()
    print(client.project)
    print("======== after calling the hook ========")
    # client = hook.get_client()
    # dataset_id = "pay-staging-386908.pay_offload_db_dev"
    # table_id = f"{dataset_id}.chk"
    
    # # Ensure dataset exists
    # ensure_dataset_exists(client, dataset_id)
    
    # # Example data
    # data = [
    #     {"name": "John Doe", "age": 30},
    #     {"name": "Jane Doe", "age": 25},
    # ]
    
    # dataframe = pd.DataFrame(data)
    # job_config = bigquery.LoadJobConfig(
    #     autodetect=True,  # Automatically infer schema from the data
    #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Use WRITE_TRUNCATE to overwrite existing data
    # )
    
    # job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)
    # job.result()  # Wait for the job to complete

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'postgres_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
)

upsert_data_task = PythonOperator(
    task_id='upsert_data',
    python_callable=upsert_data,
    provide_context=True,
    dag=dag,
)

upsert_data_task
