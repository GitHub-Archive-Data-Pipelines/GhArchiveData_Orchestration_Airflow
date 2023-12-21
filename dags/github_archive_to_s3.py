# Create a airflow dag to extract data from github archive and save it to s3
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from github import Github
import json
import boto3

# Define default_args dictionary to set default parameters for the DAG
default_args = {
    'owner': 'your_name',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'github_archive_to_s3',
    default_args=default_args,
    description='Extract data from GitHub Archive and save to S3',
    schedule_interval=timedelta(days=1),  # Set the desired schedule interval
)

# Function to extract data from GitHub Archive
def extract_data():
    # Your GitHub personal access token
    github_token = 'YOUR_GITHUB_TOKEN'
    g = Github(github_token)

    # Specify the GitHub Archive repository
    repo = g.get_repo('igrigorik/githubarchive')

    # Example: Get events for January 1, 2023
    events = repo.get_archive_link('tarball', {'path': '2023-01-01-12.json.gz'})

    # Process the events as needed
    # (Replace this with your actual data processing logic)

    return events

# Function to save data to S3
def save_to_s3(**kwargs):
    events = kwargs['task_instance'].xcom_pull(task_ids='extract_data_task')

    # Your AWS credentials and S3 bucket details
    aws_access_key_id = 'YOUR_AWS_ACCESS_KEY_ID'
    aws_secret_access_key = 'YOUR_AWS_SECRET_ACCESS_KEY'
    s3_bucket = 'YOUR_S3_BUCKET'

    # Initialize an S3 client
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

    # Specify the S3 key where data will be stored
    s3_key = 'github_archive_data.json'

    # Convert events to JSON and upload to S3
    s3.put_object(Body=json.dumps(events), Bucket=s3_bucket, Key=s3_key)

# Define tasks in the DAG
extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag,
)

save_to_s3_task = PythonOperator(
    task_id='save_to_s3_task',
    python_callable=save_to_s3,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
extract_data_task >> save_to_s3_task
