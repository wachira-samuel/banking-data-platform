# Real-Time Banking Transaction Monitoring and Fraud Detection Platform.

This project showcase my implementation of an enterprise banking data platform that processes transactions in real-time, validating data quality, detecting fraud and storing data in cloud storage + warehouse, orchestrating ETL and provide analytics dashboard for fraud monitoring and business reporting. This platform is built using a prouction-style data engineering architecture using Kafka, Spark, Airflow, Google Cloud Platform, PostgreSQL, Power BI , Docker and Machine Learning.

The goal is demonstrate how enterprise banking systems handle real-time streaming data piplines, compliance reporting, fraud detection and anlytics architecture.

# 1. Problem Statement

Banks process thousands of transactions every second through Mobile Banking, ATM withdrawals, POS Payments, Internet Banking and Third Party Payment Systems. This creates a several engineering challenges. 

The bank needs a system that can process transactions in real time, detect suspicious transactions immediately, validate transaction quality before storage, store raw and processed transactions data securely, support fraud detection and compliance reporting, handle large transaction volumes reliably and finally generate analytics dashboard for business teams.

This project solves that problem by building an  end-to-end banking data platform.

# 2. Architecture Overview

The platform processes transaction data through multiple stages

    Data Sources
        ↓
    Transaction Generator
        ↓
    Kafka Producer
        ↓
    Apache Kafka Cluster (Streaming Topic)
        ↓
    Spark Streaming Processing (PySpark)
        ↓
    Data Validation (Great Expectations)
        ↓
    Google Cloud Storage (Raw/Processed Data)
        ↓
    Cloud Dataflow ETL Processing
        ↓
    BigQuery Data Warehouse (Core Data Marts)
        ↓
    Apache Airflow Orchestration
        ↓
    Fraud Detection Models
        ↓
    Power BI Dashboards & Reporting

**Architecture Diagram**

<img width="1536" height="1024" alt="Image Jun 15, 2026, 02_32_23 PM" src="https://github.com/user-attachments/assets/b65fc257-39cc-4cea-afb7-77d822a0337d" />

# 3. Technology Stack

`a) Programming`:

• Python

• SQL

• Bash

`b) Data Streaming`:

• Apache Kafka

• Kafka Producer 

• Kafka Consumer

 For Real-time Transaction ingestion.

**Output**
<img width="1920" height="820" alt="producer py" src="https://github.com/user-attachments/assets/143cbc7e-3e11-4959-8ea6-8277ec061ed5" />

<img width="1916" height="996" alt="consumer py" src="https://github.com/user-attachments/assets/958e3e8e-dbeb-448c-bacb-387cd8191891" />

`c) Big data Processing`:

• Apache Spark

• PySpark 

 For Streaming processes and large-scale transaction transformation.

`d) Workflow Orchestration`:
Apache Airflow

 For Scheduling ETL pipleines and Manage dependencies.

`e) Cloud Infrastructure`:

• Google Cloud Storage(GCS)

• Cloud Dataflow

• Big Query

 For Raw data storage, data transformation pipelines and Sclable cloud data warehouse for analytics.

`f) Databases`:

• PostgreSQL

<img width="1920" height="615" alt="image" src="https://github.com/user-attachments/assets/eba3d70a-a679-42fa-b5a0-adbd09fdf883" />

<img width="1745" height="1009" alt="image" src="https://github.com/user-attachments/assets/d152aab0-bfd6-4a35-a321-162cdb29668c" />

<img width="1781" height="1025" alt="image" src="https://github.com/user-attachments/assets/ead5b27a-8ef6-4a23-a885-021df14cdb75" />


• BigQuery

 For Structured transactional data storage, Analytical Querying and reporting, Data marts for fraud analysis and business intelligence.

`g) Analytics & BI`:
Power BI

For fraud monitoring, business reporting and executive dashboards.

`h) Machine Learning`:
Scikit-learn

For fraud detection model

`i) DevOps`:

• Docker

• Git

For Deployment and CI/CD automation.

# 4. Project Folder Structure

    banking-data-platform/

    src/
     ├── producer/
     ├── streaming/
     ├── validation/
     ├── storage/
     ├── etl/
     ├── fraud_detection/
     ├── monitoring/

    dags/
     ├── transaction_pipeline_dag.py

    sql/
     ├── create_tables.sql

    dashboards/
     ├── powerbi_dashboard.pbix

    tests/
     ├── test_etl.py

# 5. Data Pipeline Flow

The pipeline follow these stages.

**Step 1: Transaction generation**

A python Script simulates banking transactions.

Example:

     {
    "transaction_id": "TX1001",
    "customer_id": "C1200",
    "amount": 25000,
    "merchant": "Naivas",
    "channel": "Mobile Banking",
    "location": "Nairobi"
    }

File:

    src/producer/transaction_generator.py

**Step 2: Kafka Streaming**

The producer sends transactions to Kafka.

Kafka Topic:

    bank_transactions

File:

    src/producer/producer.py

**`Purpose:`** Real-time event streaming.

**Step 3: Spark Consumer**

Spark contiuously consume transaction streams.

Tasks:

• Read Kafka messages

• Parse JSON

• Filter invalid records

• Transform transaction fields.

File:

    src/streaming/consumer.py

**Step 4: Data Validation**

Validate incoming transaction quality.

Checks performed:

• Null account IDs

• Duplicate transactio IDs

• Invalid timestamps

• Negative transaction amounts

`Tools:`Great Expectations.

`Purpose:`Ensure high-quality data

**Step 5: Cloud Storage**

Stpre raw and processed transaction data in **Google Cloud Storage (GCS)**.

Storage:

    gs://bank-transactions/raw/
    gs://bank-transactions/processed/

`Tools:` Google Cloud Storage (GCS)

`Purpose`: Centralized cloud storage for incoming banking transactions, Store processed parquet files for downstream analytics and raw transaction data lake storage.

**Step 6: ETL Transformation**

Transform raw transaction data into analytics-ready datasets.

`Tables Created;`

    customer_transactions
    merchant_summary
    daily_transactions
    fraud_alerts

`Tools`:
• Google Cloud Dataflow

• dbt

• Apache Spark

`Purpose`: Clean and validate transaction data, transform raw data into anlaytics-ready datasets and prepares fraud detection for machine learning models.

**Step 7: Data Warehouse**

Load transformed datasets into **BigQuery Data Warehouse**

`Warehouse Tables`:

    fact_transactions  
    dim_customers  
    dim_merchants  
    fact_fraud_alerts  
    dim_accounts  
    dim_date

`Tools`:
BigQuery

`Purpose`:

• Centralized analytical data warehouse.

• Store fact and dimension tables for reporting.

• Support fraud monitoring and real-time analytics queries.

• Enable dashboard reporting and machine learning workloads.

**Step 8: Fraud Detection**

Machine laerning model detects suspicious transactions.

Features used:

• Transaction amount

• Frequency transactions

• Merchant patterns

• Geographic location cahanges

Model:

    Isolation Forest

`Purpose`:
Farud detetction

**Step 9: Dashboard Reporting**

Power BI reads warehouse data.

Users can monitor:

• Fraud alerts

• Failed transactions

• Daily transaction volume

• Merchant activity

`Purpose`:
Business Intelligence

# 6. Deployment Instructions 

**Clone repository**

    git clone https://github.com/wachira-samuel/banking-data-platform

**Create Virtual environment**

    python -m venv venv

Activate environment.

Linux/Ubuntu/Mac

    source venv/bin/activate

Windows

    venv\Scripts\activate

**Install Dependencies**

    pip install -r requirements.txt

**Start Docker Services**

    docker compose up
This starts:

• Kafka

• Zookeeper

• PostgreSQL

• Spark

• Airflow

**Start Kafka Producer**

    python src/producer/producer.py

**Start Spark Consumer**

    spark-submit src/streaming/consumer.py

**Start Airflow**

    airflow standalone

**Run ETL Pipeline**

    python src/etl/etl_pipeline.py

# 7. Power BI Dashboard

The dashboard provides transaction monitoring and fraud analytics.

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

# 9. Kafka Metrics Monitoring

Kafka metrics help monitor system health.

Important metrics:

**a) Producer throughput**

Tracks messages sent per second

Example:

    Messages/sec = 1200

**b) Consumer Lag**

Tracks delay in processing messages.

Example:

    Lag = 15 messages

**c) Broker health**

Track active brokers.

Example:

    3 Active Brokers

**d) Topic size**

Tracks volume of transaction messages.

Example:

    Topic = 1.5 GB

**e) Failed Messages**

Track messages that failed delivery.

Example:
        
    Failed messages = 4

`Monitoring tools:`

• Kafka UI

• Grafana

    

