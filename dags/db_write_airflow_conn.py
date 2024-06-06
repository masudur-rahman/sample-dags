import datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="db_write_airflow_conn",
    default_args=default_args,
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
    tags=["cloudsql", "postgres"]
) as dag:

    # Task to create the pet table
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="pg-conn",
        sql="sql/pet_schema.sql",
    )

    # Task to populate the pet table
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="pg-conn",
        sql="sql/pet_insert.sql",
    )

    # Set task dependencies
    create_pet_table >> populate_pet_table
