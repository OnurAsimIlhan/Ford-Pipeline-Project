from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import logging
import traceback
import sys
import os
from elasticsearch import Elasticsearch, helpers
import csv
import random
def retrieve_data():
    print("Retrieving data from the source")
    pass

def convert_bag_to_csv():
    #run bash script from another container
    pass

def create_elasticsearch_connection():
    ELASTIC_PASSWORD = "elastic"
    CA_CERTS_PATH = "/opt/airflow/cert/ca.crt"
    client = Elasticsearch(
        "https://es01:9200",
        ca_certs=CA_CERTS_PATH,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    return client

def process_data():
    pass  # Placeholder for data processing logic

def convert_time_field(row):
    if 'Time' in row:
        try:
            timestamp = float(row['Time'])
            row['@timestamp'] = datetime.fromtimestamp(timestamp)
        except ValueError:
            logging.error(f"Failed to convert Time field for row: {row}")
    
    for key, value in row.items():
        if key != 'Time' and key != '@timestamp':
            if isinstance(value, list):
                logging.warning(f"Value for field '{key}' is a list and cannot be converted to float: {value}")
                continue
            try:
                row[key] = float(value)
            except ValueError:
                logging.warning(f"Failed to convert field '{key}' to float: {value}")
                continue

    return row

def push_data_to_elasticsearch():
    try:
        es = create_elasticsearch_connection()
        if es is None:
            raise ValueError("Elasticsearch connection could not be established.")

        base_dir = '/opt/airflow/data'
        sub_dirs = [os.path.join(base_dir, d) for d in os.listdir(base_dir) if os.path.isdir(os.path.join(base_dir, d))]
        
        logging.info(f"Found {len(sub_dirs)} sub-directories in {base_dir}")
        
        for data_dir in sub_dirs:
            logging.info(f"Processing directory: {data_dir}")
            csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
            logging.info(f"Found {len(csv_files)} CSV files in {data_dir}")
            
            for file_name in csv_files:
                file_path = os.path.join(data_dir, file_name)
                index_name = file_name.replace('.csv', '')
                
                logging.info(f"Processing file: {file_name} into index: {index_name}")
                
                with open(file_path) as f:
                    reader = csv.DictReader(f)
                    actions = [
                        {
                            "_index": index_name,
                            "_source": convert_time_field(row)
                        }
                        for row in reader
                    ]
                    helpers.bulk(es, actions)
                    logging.info(f"Data from {file_name} in {data_dir} pushed to Elasticsearch successfully")
                    
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Failed to push data to Elasticsearch: {str(e)}")

start_date = datetime(2022, 10, 19, 12, 20)

default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

with DAG('pipeline_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    data_retrieve = PythonOperator(
        task_id='data_retrieve',
        python_callable=retrieve_data,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
    
    convert_bag_files = PythonOperator(
        task_id='convert_bag_files',
        python_callable=convert_bag_to_csv,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )

    data_processing = PythonOperator(
        task_id='data_processing',
        python_callable=process_data,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )
    
    push_data = PythonOperator(
        task_id='push_data',
        python_callable=push_data_to_elasticsearch,
        provide_context=True,
        retries=1,
        retry_delay=timedelta(seconds=15)
    )

    data_retrieve >> convert_bag_files >> data_processing >> push_data
