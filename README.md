# Airbnb Analytics Data Pipeline

## Overview

This project implements an **end-to-end data pipeline for Airbnb listings data**, 

The pipeline ingests raw Airbnb data, processes it using **Apache Beam (Dataflow)**, orchestrates workflows with **Apache Airflow**, and transforms analytics-ready models using **dbt**.

The system is fully containerized with **Docker**, enabling reproducibility, portability, and quick onboarding.

---

## Architecture

**High-level flow:**

1. **Airflow** orchestrates the pipeline
2. **Apache Beam (Dataflow)** performs ETL and ingestion
3. **Raw & staged data** is written to the warehouse
4. **dbt** builds incremental analytics models
5. **Airflow DAGs** manage dependencies and execution

---

## Project Structure

## Project Structure

```text
project-root/
├── airflow/                     # Airflow docker-compose setup + DAGs
│   ├── dags/
│   ├── plugins/
│   └── docker-compose.yml
│
├── airbnb_etl/
│   ├── airbnb_data/             # Data for GCS
│   ├── dataflow/                # Apache Beam / Dataflow job
│   └── Dockerfile
│
├── airbnb_dbt/                  # dbt project
│   ├── models/
│   ├── macros/
│   ├── snapshots/
│   └── Dockerfile
│
├── scripts/
│   └── bootstrap_pipeline.sh    # One-command pipeline setup
│
└── README.md

---

## Technologies Used

- **Apache Airflow** – Workflow orchestration
- **Apache Beam / Google Dataflow** – ETL processing
- **dbt** – Analytics engineering & transformations
- **Docker & Docker Compose** – Containerization
- **GCP/BigQuery** – Cloud Data Warehouse
- **Python** – ETL logic & Airflow DAGs
- **SQL** – Analytics models


## Local Setup (Fast Start)

### Prerequisites

- Docker
- Docker Compose
- Git
- GCP credentials for cloud execution

---

### Setup Pipeline

From the project root:

```bash
chmod +x scripts/bootstrap_pipeline.sh
./scripts/bootstrap_pipeline.sh
```
Access Airflow UI
Once running:
http://localhost:8080

Trigger Airflow DAG in the UI manually with input_file: "Your Airnbnb gcs Input File"