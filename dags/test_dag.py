from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from elasticsearch import Elasticsearch, helpers
import csv
import random
import logging
import traceback
import sys
import os
import zipfile
import subprocess

# Creae an Elasticsearch connection on port 9200
def create_elasticsearch_connection():
    ELASTIC_PASSWORD = "elastic"
    CA_CERTS_PATH = "/opt/airflow/cert/ca.crt"
    client = Elasticsearch(
        "https://es01:9200",
        ca_certs=CA_CERTS_PATH,
        basic_auth=("elastic", ELASTIC_PASSWORD)
    )
    return client

# Elastic search dynamic mapping fails on csv data, this function is used to convert the data types of the fields
def mapping_setup(row):
    if 'Time' in row:
        try:
            timestamp = float(row['Time'])
            row['@timestamp'] = datetime.fromtimestamp(timestamp)
        except ValueError:
            logging.error(f"Failed to convert Time field for row: {row}")
    
    if 'gps_longitude' in row and 'gps_latitude' in row:
        try:
            longitude = float(row['gps_longitude'])
            latitude = float(row['gps_latitude'])
            row['geopoint'] = {'lat': latitude, 'lon': longitude}
            del row['gps_longitude']
            del row['gps_latitude']
        except ValueError:
            logging.error(f"Failed to convert GPS fields for row: {row}")
    
    for key, value in row.items():
        if key not in ['Time', '@timestamp', 'geopoint']:
            if isinstance(value, list):
                logging.warning(f"Value for field '{key}' is a list and cannot be converted to float: {value}")
                continue
            try:
                row[key] = float(value)
            except ValueError:
                logging.warning(f"Failed to convert field '{key}' to float: {value}")
                continue

    return row

# This function handles the logic to choose the input mode, default is set as 'batch'
def choose_input_mode(**kwargs):
    mode = kwargs['dag_run'].conf.get('mode', 'batch')
    if mode == 'batch':
        return 'retrieve_batch_data'
    elif mode == 'stream':
        return 'create_kafka_topic'
    else:
        raise ValueError("Invalid mode: choose either 'batch' or 'stream'")

def retrieve_batch_data():
    data_dir = '/opt/airflow/data'
    
    if not os.path.exists(data_dir):
        raise FileNotFoundError(f"The directory {data_dir} does not exist.")
    
    zip_files = [f for f in os.listdir(data_dir) if f.endswith('.zip')]
    sub_dirs = [d for d in os.listdir(data_dir) if os.path.isdir(os.path.join(data_dir, d))]
    
    if not zip_files and not sub_dirs:
        raise ValueError("No zip files or directories found in the data folder.")
    
    print("Batch data retrieved successfully.")
    return zip_files, sub_dirs

def unzip_data():
    data_dir = '/opt/airflow/data'
    zip_files = [f for f in os.listdir(data_dir) if f.endswith('.zip')]

    if not zip_files:
        print("No zip files found in the data directory.")
        return

    for zip_file in zip_files:
        zip_path = os.path.join(data_dir, zip_file)
        extract_dir = os.path.join(data_dir, zip_file.replace('.zip', ''))

        print(f"Unzipping {zip_file} into {extract_dir}")

        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        print(f"Successfully unzipped {zip_file}")

    # Optionally remove zip files after extraction
    for zip_file in zip_files:
        os.remove(os.path.join(data_dir, zip_file))

def convert_bag_to_csv():
    # data_dir = '/opt/airflow/data'
    # output_folder = os.path.join(data_dir, 'csv_output')
    # os.makedirs(output_folder, exist_ok=True)

    # # Find all .bag files in the data directory
    # bag_files = glob.glob(os.path.join(data_dir, '**', '*.bag'), recursive=True)

    # # Process each bag file
    # for bag_file in bag_files:
    #     print(f"Processing bag file: {bag_file}")
        
    #     # Read the bag file
    #     b = bagreader(bag_file)
        
    #     # Iterate through each topic
    #     for topic in b.topics:
    #         # Read messages from the topic
    #         data = b.message_by_topic(topic)
            
    #         try:
    #             # Convert to DataFrame, skipping bad lines
    #             df = pd.read_csv(data, on_bad_lines="skip")
                
    #             # Save DataFrame to CSV inside the output folder
    #             csv_filename = os.path.join(output_folder, f"{os.path.basename(bag_file).replace('.bag', '')}_{topic.replace('/', '_')}.csv")
    #             df.to_csv(csv_filename, index=False)
                
    #             print(f"CSV file created for topic: {topic} in bag file: {bag_file}")
    #         except pd.errors.ParserError as e:
    #             print(f"Error parsing data for topic {topic} in bag file {bag_file}: {e}")

    # print("CSV files creation process completed.")
    pass

def preprocess_data():
    print("Preprocessing data...")

def push_to_elasticsearch_batch():
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
                            "_source": mapping_setup(row)
                        }
                        for row in reader
                    ]
                    helpers.bulk(es, actions)
                    logging.info(f"Data from {file_name} in {data_dir} pushed to Elasticsearch successfully")
                    
    except Exception as e:
        traceback.print_exc(file=sys.stderr)
        logging.error(f"Failed to push data to Elasticsearch: {str(e)}")

def create_kafka_topic():
    pass

def consume_from_kafka():
    pass

def pyspark_processing():
    pass

def push_to_elasticsearch_stream():
    pass

def ml_training():
    print("Running ML training...")

def anomaly_detection():
    print("Performing anomaly detection...")


start_date = datetime(2023, 10, 19, 12, 20)

default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

dag = DAG(
    'data_pipeline',
    default_args=default_args,
    schedule_interval=None,
)

choose_input_mode = BranchPythonOperator(
    task_id='choose_input_mode',
    python_callable=choose_input_mode,
    provide_context=True,
    dag=dag,
)

retrieve_batch_data = PythonOperator(
    task_id='retrieve_batch_data',
    python_callable=retrieve_batch_data,
    dag=dag,
)

unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data,
    dag=dag,
)

convert_bag_to_csv = PythonOperator(
    task_id='convert_bag_to_csv',
    python_callable=convert_bag_to_csv,
    dag=dag,
)

preprocess_data = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    dag=dag,
)

push_to_elasticsearch_batch = PythonOperator(
    task_id='push_to_elasticsearch_batch',
    python_callable=push_to_elasticsearch_batch,
    dag=dag,
)

create_kafka_topic = PythonOperator(
    task_id='create_kafka_topic',
    python_callable=create_kafka_topic,
    dag=dag,
)

consume_from_kafka = PythonOperator(
    task_id='consume_from_kafka',
    python_callable=consume_from_kafka,
    dag=dag,
)

pyspark_processing = PythonOperator(
    task_id='pyspark_processing',
    python_callable=pyspark_processing,
    dag=dag,
)

push_to_elasticsearch_stream = PythonOperator(
    task_id='push_to_elasticsearch_stream',
    python_callable=push_to_elasticsearch_stream,
    dag=dag,
)

ml_training_task = PythonOperator(
    task_id='ml_training',
    python_callable=ml_training,
    dag=dag,
)

anomaly_detection_task = PythonOperator(
    task_id='anomaly_detection',
    python_callable=anomaly_detection,
    dag=dag,
)



choose_input_mode >> [retrieve_batch_data, create_kafka_topic]
retrieve_batch_data >> unzip_data >> convert_bag_to_csv >> [preprocess_data, ml_training_task]
preprocess_data >> push_to_elasticsearch_batch
ml_training_task >> anomaly_detection_task >> push_to_elasticsearch_batch
create_kafka_topic >> consume_from_kafka >> pyspark_processing >> push_to_elasticsearch_stream
