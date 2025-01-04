from airflow import DAG
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobTrigger
from datetime import datetime, timedelta


# Define the DAG
with DAG(
    dag_id='trigger_dbt_job_saif',
    schedule_interval=None,
    description ='triggers a dbt job to do the modelling',
    start_date = datetime(2025, 1, 1)
) as dag:
    

    trigger_dbt_job = DbtCloudRunJobTrigger(
        task_id='trigger_dbt_job',
        conn_id='dbt_cloud_default',  # Specify your Airflow connection ID
        run_id=70471823413982,  # Replace with your dbt Cloud job ID
        account_id = 70471823411227,
        end_time = 900,
        poll_interval=30,  # Poll every 30 seconds for status updates
    )
    

    

    
