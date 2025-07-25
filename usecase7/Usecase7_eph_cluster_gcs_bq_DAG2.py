import os
import datetime
from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
   DataprocCreateClusterOperator,
   DataprocSubmitJobOperator,
   DataprocDeleteClusterOperator,
)
from airflow.utils.dates import days_ago

PROJECT_ID = "deai-2025"
CLUSTER_NAME="singlenode-dpeph-cluster"
REGION = "us-central1"
ZONE = "us-central1-a"
PYSPARK_CODE1_URI = "gs://jayashree-first-bucket/code_Usecase6_step1_gcs_bq.py"

default_args = {
    "start_date": days_ago(1),
    "project_id": PROJECT_ID,
}

with models.DAG(
    "usecase-7-DAG-To-Create-Submit-PySpark-Task-Delete-EPH-Cluster",
    default_args=default_args,
    schedule_interval=datetime.timedelta(days=1),
) as dag:
   
    # Define cluster configuration for a single-node cluster
    cluster_config = {
        "master_config": {
            "num_instances": 1,
            "machine_type_uri": "e2-standard-2",
            "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 50},
        },
        "worker_config": {
            "num_instances": 0,  # Single-node cluster doesn't have worker nodes
        },
        "software_config": {
            "optional_components": [],            
            "image_version": "2.1-debian11",
        },
    }

    # Define the cluster creation task
    create_cluster = DataprocCreateClusterOperator(
        task_id='create_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=cluster_config,
    )

    # Define the PySpark job parameters
    pyspark_job_params = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_CODE1_URI},
    }

    # Define the PySpark job submission task
    submit_pyspark_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job", 
        job=pyspark_job_params, 
        region=REGION, 
        project_id=PROJECT_ID,
    )

    # Define the cluster deletion task
    delete_cluster = DataprocDeleteClusterOperator(
        task_id='delete_cluster',
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        trigger_rule='all_done',  # Ensure cluster deletion runs even if the job fails
    )

    # Set task dependencies
    create_cluster >> submit_pyspark_job >> delete_cluster