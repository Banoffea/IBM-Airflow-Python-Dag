from airflow.models import DAG
from airflow.operators.python import PythonOperator

import pendulum
from datetime import timedelta
import requests

BASE_PATH = "/opt/airflow/data"

input_file = f"{BASE_PATH}/web-server-access-log.txt"
extracted_file = f"{BASE_PATH}/extracted.txt"
transformed_file = f"{BASE_PATH}/transformed.txt"
output_file = f"{BASE_PATH}/capitalized.txt"


def download_file():
    url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"
    response = requests.get(url, allow_redirects=True)
    with open(input_file, 'wb') as file:
        for line in response:
            file.write(line)
    print("File downloaded successfully")

def extract():
    with open(input_file, 'r') as infile, open(extracted_file, 'w') as outfile:
        for line in infile:
            fields = line.split('#')
            if len(fields) >= 4:
                field_1 = fields[0]
                field_4 = fields[3]
                outfile.write(field_1 + "#" + field_4 + "\n")

def transform():
    with open(extracted_file, 'r') as infile, open(transformed_file, 'w') as outfile:
        for line in infile:
            transformed_line = line.upper()
            outfile.write(transformed_line)


def load():
    with open(transformed_file, 'r') as infile, open(output_file, 'w') as outfile:
        for line in infile:
            outfile.write(line)

# def load():
#     with open(transformed_file, 'r') as infile:
#         data = infile.read()
    
#     with open(output_file, 'w') as outfile:
#         outfile.write(data)


default_args = {
    'owner': 'Your name',
    'start_date': pendulum.today("UTC"),
    'email': ['your email'],
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl-server-logs-dag',
    default_args = default_args,
    description = 'First PY DAG',
    schedule="* */1 * * *"
)

execute_download = PythonOperator(
    task_id='download',
    python_callable=download_file,
    dag=dag
)

execute_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

execute_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

execute_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

execute_download>>execute_extract>>execute_transform>>execute_load