# ETL pipeline with open data (CSV to Parquet to BigQuery)

This project demonstrates a production-style ETL pipeline that:

1. Extracts open dataset CSV files
2. Cleans and transforms data
3. Converts to Parquet format
4. Uploads to Google Cloud Storage









5. Loads data into BigQuery


**Project Architecture:**

    Open Dataset (CSV)
            │
            ▼
    Extract Script (Python)
            │
            ▼
    Transform (Clean + Feature Engineering)
            │
            ▼
    Convert to Parquet
            │
            ▼
    Upload to GCS
            │
            ▼
    Load to BigQuery
            │
            ▼
    SQL Analytics


**Tech Stack:**

1. Python
2. Apache Airflow
3. Google Cloud Storage
4. BigQuery
5. Docker
6. Parquet
