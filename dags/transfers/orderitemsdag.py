from airflow import DAG
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime, timedelta


# Define the DAG
with DAG(
    dag_id='orderitems_postgres_to_bigquery_via_gcs_saif_amir',
    schedule_interval=None,
    description ='Loading Data from postgres to bigquery via gcs',
    start_date = datetime(2025, 1, 1)
) as dag:

    # transfer data from PostgreSQL to GCS
    transfer_to_gcs = PostgresToGCSOperator(
        task_id='transfer_postgres_to_gcs',
        postgres_conn_id='postgres_conn',  # Your PostgreSQL connection ID
        sql='SELECT * FROM order_items;',  # Your SQL query
        bucket='ready-d25-postgres-to-gcs',  # Your GCS bucket name
        filename='seif/order_items/order_items.csv',  # GCS object path
        export_format='csv',  # Export format (csv, json, etc.)
    )
   

    # load data from GCS to BigQuery
    load_to_bigquery = GCSToBigQueryOperator(
        task_id='load_gcs_to_bigquery',
        bucket='ready-d25-postgres-to-gcs',  # Your GCS bucket name
        source_objects=['seif/order_items/order_items.csv'],  # GCS object path
        destination_project_dataset_table='ready-de-25.playground.order_items_saif',  # BigQuery table
        source_format='CSV',  # Format of the data in GCS
        skip_leading_rows=1,  # Skip header row if necessary
        autodetect=True,  # Automatically detect schema
    )

    # Set task dependencies
    transfer_to_gcs >> load_to_bigquery
