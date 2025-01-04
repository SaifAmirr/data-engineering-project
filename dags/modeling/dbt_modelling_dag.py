from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime, timedelta


# Define the DAG
with DAG(
    dag_id='trigger_dbt_job_saif',
    schedule_interval=None,
    description ='triggers a dbt job to do the modelling',
    start_date = datetime(2025, 1, 1)
) as dag:
    

    trigger_dbt_job = DbtCloudRunJobOperator(
        task_id='trigger_dbt_job',
        conn_id='dbt_cloud_default',  # Specify your Airflow connection ID
        job_id=70471823413982,  # Replace with your dbt Cloud job ID
        timeout=600,  # Timeout for the job execution (in seconds)
        poll_interval=30,  # Poll every 30 seconds for status updates
    )
    

    

    
