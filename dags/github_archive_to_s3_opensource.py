# Create a airflow dag to extract data from github archive and save it to s3
from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.http.operators.http import SimpleHttpOperator


# Define default_args dictionary to set default parameters for the DAG
default_args = {
    "owner": "your_name",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATE = "{{ ds }}"
HOUR = "{{ dag_run.logical_date.strftime('%H') }}"

# Initialize the DAG
with DAG(
    "github_archive_to_s3_opensource",
    catchup=False,
    default_args=default_args,
    description="Extract data from GitHub Archive and save to S3",
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
) as dag:
    # Define the 'start' task
    start = EmptyOperator(task_id="start")

    # Call service to ingest data from GitHub Archive
    extract_data = SimpleHttpOperator(
        task_id="extract_data",
        endpoint="2015-03-31/functions/function/invocations",
        data=json.dumps({"date": DATE, "hour": HOUR}),
        headers={"Content-Type": "application/json"},
        dag=dag,
        http_conn_id="gh-archive-data-raw-lambda",
    )

    # Define S3 sensor
    s3_sensor = S3KeySensor(
        task_id="s3_sensor",
        bucket_name="gh-archive-data-raw",
        bucket_key="*.json.gz",
        aws_conn_id="gh-archive-data-raw-s3",
        wildcard_match=True,
        poke_interval=60 * 5,
        retries=0,
        mode="reschedule",
    )


    # Define the 'end' task
    end = EmptyOperator(task_id="end")

    # Set task dependencies
    start >> extract_data >> s3_sensor >> end
