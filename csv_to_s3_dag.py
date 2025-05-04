from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import pandas as pd

FILE_PATH = "/home/mulyo/Learning/ETL_CSV_To_PostrgeSQL/CO2 Emission Country.csv"

AWS_CONN_ID = 's3_conn'

default_args = {
    'owner': 'mulyo',
    'tries': 5,
    'try_delay': timedelta(minutes=2)
}

@dag(
    dag_id = 'csv_to_s3_dag',
    description = 'ETL csv to S3 AWS',
    default_args = default_args,
    start_date = datetime(2025, 5, 3),
    schedule_interval = '@daily',
    catchup = False,
    tags=["S3 AWS", "csv"],
)

def csv_to_s3():

    @task()
    def dataExtraction(file_path):
        df = pd.read_csv(file_path)
        return df

    @task()
    def dataTransformation(df:pd.DataFrame)->pd.DataFrame:

        # remove duplicates
        deduplication_df = df.drop_duplicates()

        # remove n/a
        removena_df = deduplication_df.dropna()

        # data type correction
        # --Remove '%' from string data
        removena_df['% of global total'] = removena_df['% of global total'].str.replace('%', '', regex=False)

        # --Convert string to numeric on '% of global total' column
        removena_df['% of global total'] = pd.to_numeric(removena_df['% of global total'], errors='raise')

        # --Remove ',' string from value from 'Fossil emissions 2023' column
        removena_df['Fossil emissions 2023'] = removena_df['Fossil emissions 2023'].str.replace(',', '', regex=False)

        # --Convert to numeric on column 'Fossil emissions 2023'
        removena_df['Fossil emissions 2023'] = pd.to_numeric(removena_df['Fossil emissions 2023'], errors='raise')

        # --Remove ',' 'no' string from value from 'Fossil emissions 2000' column
        removena_df['Fossil emissions 2000'] = removena_df['Fossil emissions 2000'].str.replace(',', '', regex=False)
        removena_df['Fossil emissions 2000'] = removena_df['Fossil emissions 2000'].str.replace('no', '', regex=False)

        # --Convert to numeric on column 'Fossil emissions 2000'
        removena_df['Fossil emissions 2000'] = pd.to_numeric(removena_df['Fossil emissions 2000'], errors='raise')

        # --Remove '%' '+' string from value from '% change 2000' column
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace('%', '', regex=False)
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace('+', '', regex=False)
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace("−", "-")

        # --Remove ',' 'change' string from value from '% change 2000' column
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace(',', '', regex=False)
        removena_df['% change from 2000'] = removena_df['% change from 2000'].str.replace('change', '', regex=False)

        # --Convert to numeric on column '% change 2000'
        removena_df['% change from 2000'] = pd.to_numeric(removena_df['% change from 2000'], errors='raise')

        return removena_df

    @task()
    def loadData(df:pd.DataFrame, bucket_name:str):
        
        s3 = S3Hook(aws_conn_id=AWS_CONN_ID)        
        csv_buffer = df.to_csv(index=False)
        s3_key='data/CO2 Emission Country.csv'

        s3.load_string(
            csv_buffer,
            key=s3_key,
            bucket_name=bucket_name,
            replace=True
        )

    extracted_df = dataExtraction(FILE_PATH)
    transformed_df = dataTransformation(extracted_df)
    loadData(extracted_df, bucket_name="airflow-csv-upload")

etl = csv_to_s3()
