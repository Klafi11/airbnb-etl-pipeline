"""
Airflow DAG for Beam + DBT pipeline with GCP authentication.

This DAG:
1. Validates input from dag_run.conf
2. Runs a Beam pipeline in Docker
3. Runs DBT transformations in Docker

Trigger with:
airflow dags trigger beam_dbt_pipeline --conf '{"input_file": "gs://bucket/file.csv"}'
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.exceptions import AirflowFailException
from datetime import datetime, timedelta
from docker.types import Mount
import os
import logging


# CONFIGURATION
GCP_PROJECT = os.environ.get("GCP_PROJECT_ID", "your-project-id")
GCS_TEMP_LOCATION = os.environ.get("GCS_TEMP_LOCATION")

# Path to credentials inside containers
CREDENTIALS_PATH = os.environ.get("CREDENTIALS_PATH")

# Docker configuration
DOCKER_SOCK = os.environ.get("DOCKER_SOCK")
CREDENTIALS_VOLUME = os.environ.get("CREDENTIALS_VOLUME")


# TASK FUNCTIONS
def validate_dag_config(**context):
    """
    Validate that required parameters are present in dag_run.conf.
    Returns the input_file path for downstream tasks.
    """
    dag_run_conf = context.get("dag_run").conf or {}
    
    # Check for required parameter
    input_file = dag_run_conf.get("input_file")
    if not input_file:
        raise AirflowFailException(
            "Missing required parameter 'input_file' in dag_run.conf. "
            "Trigger with: --conf '{\"input_file\": \"gs://bucket/file.csv\"}'"
        )
    
    # Validate that it's a GCS path
    if not input_file.startswith("gs://"):
        raise AirflowFailException(
            f"input_file must be a GCS path (gs://...), got: {input_file}"
        )
    
    print(f"Validated input_file: {input_file}")
    return input_file


def log_pipeline_start(**context):
    """Log pipeline execution details."""
    input_file = context["ti"].xcom_pull(task_ids="validate_input")
    logging.info("=" * 80)
    logging.info("BEAM + DBT PIPELINE STARTING")
    logging.info(f"Input File: {input_file}")
    logging.info("=" * 80)


def log_success(**context):
    """Log successful pipeline completion."""
    input_file = context["ti"].xcom_pull(task_ids="validate_input")
    logging.info("=" * 80)
    logging.info("PIPELINE COMPLETED SUCCESSFULLY")
    logging.info(f"  Input: {input_file}")
    logging.info(f"  Beam: Complete")
    logging.info(f"  DBT: Complete")
    logging.info("=" * 80)



# DAG DEFINITION

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="beam_dbt_pipeline",
    default_args=default_args,
    description="Run Beam ETL followed by DBT transformations",
    schedule=None,
    catchup=False,
    tags=["etl", "beam", "dbt", "gcp"],
) as dag:

  
    # TASK 1: Validate Input
    validate_input = PythonOperator(
        task_id="validate_input",
        python_callable=validate_dag_config,
    )


    # TASK 2: Log Pipeline Details
    log_start = PythonOperator(
        task_id="log_start",
        python_callable=log_pipeline_start,
    )

    # TASK 3: Run Apache Beam Pipeline
    run_beam_pipeline = DockerOperator(
        task_id="run_beam_pipeline",
        image="airbnb-dataflow:latest",
        docker_url=DOCKER_SOCK,
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        # Input Validation
        command=[
            # Default Option in beam activated
            "python", "main.py",
            "--input_file", "{{ ti.xcom_pull(task_ids='validate_input') }}",
            "--temp_location", GCS_TEMP_LOCATION,
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": CREDENTIALS_PATH,
            "GCP_PROJECT": GCP_PROJECT,
        },
        mounts=[
            Mount(
                #Running with DockerOperator - Source needs to be the Host, bc gets exexuted in the docker VM on Host
                source=CREDENTIALS_VOLUME,
                target="/credentials",
                type="bind",
                read_only=True,
            )
        ],
    )


    # TASK 4: Run DBT Transformations
    run_dbt_transformations = DockerOperator(
        task_id="run_dbt_transformations",
        image="airbnb-dbt:latest",
        docker_url=DOCKER_SOCK,
        network_mode="bridge",
        auto_remove="success",
        mount_tmp_dir=False,
        command=[
        "bash", "-c",
        "dbt deps --project-dir /dbt && "
        "dbt build --project-dir /dbt --profiles-dir /dbt/profiles"
        ],
        environment={
            "GOOGLE_APPLICATION_CREDENTIALS": CREDENTIALS_PATH,
            "GCP_PROJECT": GCP_PROJECT,
            "DBT_PROFILES_DIR": "/dbt/profiles",
        },
        mounts=[
            Mount(
                #Running with DockerOperator - Source needs to be the Host, bc gets exexuted in the docker VM on Host
                source=CREDENTIALS_VOLUME,
                target="/credentials",
                type="bind",
                read_only=True,
            )
        ],
    )


    # TASK 5: Log Success
    log_completion = PythonOperator(
        task_id="log_completion",
        python_callable=log_success,
    )

    # TASK DEPENDENCIES
    validate_input >> log_start >> run_beam_pipeline >> run_dbt_transformations >> log_completion