# airflow_csv_to_s3
Automate Data Ingestion csv into S3 AWS using Airflow:
1. Load necessary libs -- airflow.decorators, airflow.providers, pandas, datetime
2. Provide -- file path, s3 connection id
3. ETL flow -- data extraction -> data transformation -> load s3
4. Close connection -- automate with context
