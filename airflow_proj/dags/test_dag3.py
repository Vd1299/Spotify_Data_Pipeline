import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv



# Directory containing the CSV files and processed files tracking
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_DIRECTORY = os.path.join(BASE_DIR, 'files')
PROCESSED_FILES_PATH = os.path.join(CSV_DIRECTORY, 'processed_files.txt')
S3_BUCKET = "airflow-data-snowflake"

# Load environment variables from the .env file in the etl folder
load_dotenv(os.path.join(BASE_DIR, 'etl', '.env'))

# AWS Credentials
aws_access_key = os.getenv("AWS_ACCESS_KEY")
aws_secret_key = os.getenv("AWS_SECRET_KEY")
aws_region = os.getenv("AWS_REGION")

# Importing modules from the etl folder
from etl.extract import extract_new_files
from etl.load import load_to_s3
from etl.transform import transform_csv

# Function to update the list of processed files
def update_processed_files(processed_files):
    with open(PROCESSED_FILES_PATH, 'w') as f:
        f.write('\n'.join(processed_files))

# Extract task
def extract_task():
    if os.path.exists(PROCESSED_FILES_PATH):
        with open(PROCESSED_FILES_PATH, 'r') as f:
            processed_files = f.read().splitlines()
    else:
        processed_files = []

    new_files = extract_new_files(CSV_DIRECTORY, processed_files)
    return new_files

# Transform task
def transform_task(new_files):
    transformed_data = []
    
    for file in new_files:
        file_path = os.path.join(CSV_DIRECTORY, file)
        df = transform_csv(file_path)
        transformed_data.append((df, file))
    
    return transformed_data

# Load task
def load_task(transformed_data):
    processed_files = []

    for df, file_name in transformed_data:
        load_to_s3(S3_BUCKET, df, file_name, aws_access_key, aws_secret_key, aws_region)
        processed_files.append(file_name)
    
    update_processed_files(processed_files)

# Main ETL function
def run_etl():
    new_files = extract_task()
    if not new_files:
        print("No new files to process.")
        return

    transformed_data = transform_task(new_files)
    load_task(transformed_data)

# Define the DAG
with DAG(
    dag_id='etl_pipeline3',
    schedule_interval='*/30 * * * *',  # Run every half hour
    start_date=days_ago(1),
    catchup=False,
    tags=['etl'],
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_task',
        python_callable=run_etl,
    )

    etl_task
