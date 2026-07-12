# Real-Time Banking Transaction Monitoring and Fraud Detection Platform.

This project demonstrates an end-to-end data engineering platform for processing banking transactions in real time. It ingests streaming events with Apache Kafka, processes and validates them using PySpark, stores data in PostgreSQL and Google Cloud Storage, loads analytics-ready datasets into BigQuery, orchestrates workflows with Apache Airflow and visualizes business metrics in Power BI.

The goal is to demonstrate how enterprise banking systems handle real-time streaming data piplines, compliance reporting, fraud detection and anlytics architecture.

# 1. Problem Statement

Banks process thousands of transactions every second through Mobile Banking, ATM withdrawals, POS Payments, Internet Banking and Third Party Payment Systems. This creates a several engineering challenges. 

The bank needs a system that can process transactions in real time, detect suspicious transactions immediately, validate transaction quality before storage, store raw and processed transactions data securely, support fraud detection and compliance reporting, handle large transaction volumes reliably and finally generate analytics dashboard for business teams.

This project solves that problem by building an  end-to-end banking data platform.

## Features

- Real-time transaction streaming using Apache Kafka
- Data validation and cleansing
- Fraud detection rules
- PostgreSQL operational storage
- Google Cloud Storage data lake
- BigQuery analytics warehouse
- Apache Airflow orchestration
- Power BI dashboards
- Dockerized development environment

# 2. Architecture Overview

                       Transaction Generator
                               │
                               ▼
                        Kafka Producer
                               │
                               ▼
                Apache Kafka (bank_transactions)
                               │
                               ▼
             Spark Structured Streaming (PySpark)
                               │
               ┌───────────────┼────────────────┐
               ▼               ▼                ▼
      Data Validation   Data Transformation   Fraud Detection
               │               │                │
               └───────────────┴────────────────┘
                               │
                            Apache Airflow
                  (Pipeline Scheduling & Orchestration)
                               │
                               ▼
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                    ▼
       PostgreSQL           Google Cloud Storage     BigQuery
     (Operational Store)     (Raw & Processed)   (Analytics Warehouse)
                                                    │
                                                    ▼
                                           Power BI Dashboards


| Component                  | Purpose                                            |
| -------------------------- | -------------------------------------------------- |
| Transaction Generator      | Simulates banking transactions                     |
| Kafka Producer             | Publishes events to Kafka                          |
| Apache Kafka               | Streaming message broker                           |
| Spark Structured Streaming | Processes transaction streams in real time         |
| Data Validation            | Ensures data quality before storage                |
| Data Transformation        | Cleans and enriches transaction data               |
| Fraud Detection            | Applies fraud detection rules and risk scoring     |
| PostgreSQL                 | Stores operational transaction records             |
| Google Cloud Storage       | Stores raw and processed datasets in the data lake |
| BigQuery                   | Stores analytics-ready data for reporting          |
| Power BI                   | Business intelligence dashboards                   |
| Apache Airflow             | Schedules and orchestrates the analytical batch  workflows and data warehousing layer    |
                                          


**Architecture Diagram**

<img width="1536" height="1024" alt="Image Jun 15, 2026, 02_32_23 PM" src="https://github.com/user-attachments/assets/b65fc257-39cc-4cea-afb7-77d822a0337d" />

# 3. Technology Stack

| Category         | Technologies  |
| ---------------- | ------------- |
| Programming      | Python, SQL   |
| Streaming        | Kafka         |
| Processing       | PySpark       |
| Workflow         | Airflow       |
| Cloud            | GCS, BigQuery |
| Database         | PostgreSQL    |
| Visualization    | Power BI      |
| Containerization | Docker        |
| ML               | Scikit-learn  |

# 4. Project Folder Structure
    banking-data-platform/
        ├── airflow/
        ├── credentials/
        ├── datalake/
        ├── orchestration/
        │   ├── dags/
        │   └── scripts/
        ├── src/
        │   ├── cloud/
        │   ├── ingestion/
        │   ├── storage/
        │   └── streaming/
        ├── tests/
        ├── docker-compose.yml
        ├── requirements.txt
        └── README.md
# 5. Pipeline Workflow


## a. Generate Transactions

A Python-based transaction generator simulates real-world banking activities by creating synthetic transaction records with customer, account, merchant, amount and timestamp information. This provides a continuous stream of data for testing the pipeline.

## b. Publish to Kafka

The generated transactions are published to an Apache Kafka topic, where Kafka acts as a reliable, fault-tolerant message broker for real-time event streaming.

## c. Process with Spark

PySpark Structured Streaming consumes transaction events from Kafka and processes them in real time, enabling scalable stream processing and preparing data for downstream analytics.

## d. Validate Data

Incoming records are validated against predefined business rules and schemas to ensure data quality, consistency and completeness before further processing.

## e. Store Raw Data in Google Cloud Storage

Validated transactions are written to Google Cloud Storage as raw data, creating a scalable data lake that preserves the original records for auditing and future reprocessing.

## f. Transform for Analytics

The streaming data is cleaned, enriched and transformed into an analytics-ready format. During this stage, business metrics and derived fields are generated to support reporting and analysis.

## g. Load into BigQuery

The transformed data is loaded into Google BigQuery, where it is organized into analytical tables optimized for fast SQL queries and business intelligence workloads.

## h. Detect Fraud

Fraud detection rules analyze transaction patterns to identify potentially suspicious activities, assign risk scores, and flag transactions that require further investigation.

## i. Visualize in Power BI

Business users access interactive Power BI dashboards to monitor transaction volumes, fraud trends, customer behavior, and other key performance indicators in near real time.

# 6. Deployment Guide 

**a. Clone the repository**

    git clone https://github.com/wachira-samuel/banking-data-platform

**b. Create Virtual environment**

    python -m venv .venv

Activate environment.

Linux/Ubuntu/Mac

    source .venv/bin/activate

Windows

    .venv\Scripts\activate

**c. Install Dependencies**

Using UV(recommended):

    uv pip install -r requirements.txt

Or using pip:

    pip install -r requirements.txt

**d. Start the Platform Services**

Start all the required services using Docker Compose:

    docker compose up -d
    
This start the following services:

• Apache Kafka

• Zookeeper

• PostgreSQL

•  Apache Spark

• Apache Airflow

Verify that the containers are running:

    docker ps

# 7. Generate Transaction Data

Run the transaction generator to simulate banking transactions:

       python src/ingestion/transaction_generator.py

# 8. Publish Transactions to Kafka

Start the Kafka producer:

       python src/ingestion/producer.py

# 9. Start the Spark Streaming Pipeline

Launch the Spark Structured Streaming application:

    python src/streaming/spark_streaming.py

The pipeline will:

- Consume transactions from Kafka
- Validate incoming records
- Transform data for analytics
- Detect potentially fraudulent transactions
- Store raw and processed data in Google Cloud Storage
- Load analytics-ready data into BigQuery
- Write operational data to PostgreSQL
  
# 10. Start Airflow

    airflow standalone

If Airflow is running through Docker Compose, open:

    http://localhost:8080
    
From the Airflow UI, you can monitor and manage workflow execution.

# 11. Visualize the Data

After data has been loaded into BigQuery, connect Power BI to BigQuery to build interactive dashboards for transaction analytics and fraud monitoring.

Metrics displayed:

• Transactions per minute

• Fraud alert count

• Failed transactions

• Transaction volume by region

• Top merchants by volume

• Daily transaction trends

**Example Dashboard Layout**

        +-------------------------------------------+
        | Daily Transaction Volume                  |
        | █████████████████████                    |
        +-------------------------------------------+

        +-------------------------------------------+
        | Fraud Alerts Today                        |
        | 127 Suspicious Transactions               |
        +-------------------------------------------+

        +-------------------------------------------+
        | Transactions by Region                    |
        | Nairobi | NewYork | Kisumu | Nakuru       |
        +-------------------------------------------+

        +-------------------------------------------+
        | Failed Transactions                       |
        | 32 Failed Validations                     |
        +-------------------------------------------+


        ---------------------------------------------------------------
        | TRANSACTION MONITORING DASHBOARD                            |
        ---------------------------------------------------------------

        Total Transactions Today        Fraud Alerts Today
        124,563                         214

        ---------------------------------------------------------------

        Transaction Volume by Region

        Nairobi      ███████████████

        NewYork      ██████████

        Kisumu       ███████

        Nakuru       █████████

        ---------------------------------------------------------------

        Failed Transactions

        12 Failed Data Validations

        ---------------------------------------------------------------

        Top Merchants

        Naivas
        Carrefour
        Quickmart
        JumiaPay

        ---------------------------------------------------------------

## Project Highlights

- Built an end-to-end streaming data pipeline
- Integrated Kafka, Spark, PostgreSQL, GCS, BigQuery, and Airflow
- Implemented fraud detection rules
- Designed cloud analytics workflows
- Containerized services with Docker

## License

This project is licensed under the MIT License.

    

