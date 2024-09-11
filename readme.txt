# Docker Compose Setup

## Overview

This repository contains a Docker Compose configuration for setting up an Airflow environment with PostgreSQL, Redis, and Elasticsearch with Kibana. It also includes Metricbeat for monitoring.

## Services

### Airflow

- **Airflow Webserver**: 
  - **Port**: `8080`
  - **Description**: The web interface for Airflow.

- **Airflow Scheduler**: 
  - **Port**: Not exposed
  - **Description**: The scheduler for managing DAG runs.

- **Airflow Worker**: 
  - **Port**: Not exposed
  - **Description**: The worker processes that execute tasks.

- **Airflow Triggerer**: 
  - **Port**: Not exposed
  - **Description**: The component for triggering DAGs.

### PostgreSQL

- **Port**: `5442` (mapped to internal port `5432`)
- **Description**: Database used by Airflow for storing metadata.

### Redis

- **Port**: `6379`
- **Description**: Broker for Celery, used by Airflow for task queuing.

### Elasticsearch

- **Port**: `9200` (mapped to `${ES_PORT}`)
- **Description**: Search and analytics engine.

### Kibana

- **Port**: `5601` (mapped to `${KIBANA_PORT}`)
- **Description**: Data visualization and exploration tool for Elasticsearch.

### Metricbeat

- **Port**: Not exposed
- **Description**: Collects metrics from the Docker environment and sends them to Elasticsearch.

## Credentials

- **PostgreSQL**:
  - **Username**: `airflow`
  - **Password**: `airflow`
  - **Database**: `airflow`

- **Airflow**:
  - **Default Username**: `airflow` (can be set with `_AIRFLOW_WWW_USER_USERNAME` environment variable)
  - **Default Password**: `airflow` (can be set with `_AIRFLOW_WWW_USER_PASSWORD` environment variable)

- **Elasticsearch**:
  - **Elastic Password**: `${ELASTIC_PASSWORD}`
  - **Kibana Password**: `${KIBANA_PASSWORD}`

- **Kibana**:
  - **Username**: `kibana_system`
  - **Password**: `${KIBANA_PASSWORD}`

## Environment Variables

Ensure you set the following environment variables in your `.env` file:

- **For Elasticsearch and Kibana**:
  - `STACK_VERSION`: Version of the Elasticsearch and Kibana images
  - `ELASTIC_PASSWORD`: Password for the Elasticsearch `elastic` user
  - `KIBANA_PASSWORD`: Password for the Kibana `kibana_system` user
  - `CLUSTER_NAME`: Name of the Elasticsearch cluster
  - `LICENSE`: Type of Elasticsearch license
  - `ES_PORT`: Port for Elasticsearch
  - `KIBANA_PORT`: Port for Kibana
  - `ES_MEM_LIMIT`: Memory limit for Elasticsearch
  - `KB_MEM_LIMIT`: Memory limit for Kibana
  - `ENCRYPTION_KEY`: Encryption key for Kibana


## Setup and Initialization

Run "docker compose up -d" inside of the project

Converting bag to csv is not working due to dependincy issue inside airflow. Run that function locally. 

Airflow DAG fetches data from /data folder. If you can choose dependincy issue of bagpy, it will work for a zip file, extract and push within the dag

I used my own trial certificate for elastic, so if you get certification error. generate a certificate within the airflow image.

