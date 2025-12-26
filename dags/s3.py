from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import pandas as pd
from io import StringIO

BUCKET_NAME = 'airflow-demo-amisha'
S3_KEY = 'sales.csv'

default_args = {
    'start_date': datetime(2025, 1, 1)
}

with DAG(
    dag_id='s3_to_postgres_etl',
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    # -----------------------------------
    # Task 0: Create table if not exists
    # -----------------------------------
    @task()
    def create_sales_table():
        pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')
        table_created = """
            CREATE TABLE IF NOT EXISTS sales (
                order_id INT PRIMARY KEY,
                order_date DATE,
                customer_id VARCHAR(10),
                region VARCHAR(50),
                product VARCHAR(50),
                quantity INT,
                unit_price NUMERIC,
                total_amount NUMERIC
            );
        """
        pg_hook.run(table_created)


    # -----------------------------------
    # Task 1: Extract from S3
    # -----------------------------------
    @task()
    def extract_from_s3():
        s3_hook = S3Hook(aws_conn_id='my_aws')
        csv_string = s3_hook.read_key(
            key=S3_KEY,
            bucket_name=BUCKET_NAME
        )
        return csv_string

    # -----------------------------------
    # Task 2: Transform data
    # -----------------------------------
    @task()
    def transform_data(csv_string: str):
        df = pd.read_csv(StringIO(csv_string))

        # Basic cleaning & transformations
        df = df[df['quantity'] > 0]
        df['total_amount'] = df['quantity'] * df['unit_price']
        df = df.drop_duplicates()

        # Mean imputation for numeric columns
        numeric_cols = df.select_dtypes(include=['int64', 'float64']).columns
        df[numeric_cols] = df[numeric_cols].fillna(df[numeric_cols].mean())

        return df.to_json(orient='records')

    # -----------------------------------
    # Task 3: Load into Postgres
    # -----------------------------------
    # @task()
    # def load_data(json_data: str):
    #     df = pd.read_json(json_data)

    #     pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')
    #     insert_query = '''
    #         INSERT INTO table_created (order_id,order_date,customer_id,region,product,quantity,unit_price,total_amount) 
    #         values (%s,%s,%s,%s,%s,%s,%s,%s)'''
        
    #     data_para = (table_created['order_id'],
    #                  table_created['order_date'])
        
    #     pg.hook.run(insert_query,parameters=data_para)
    # @task()
    # def load_data(json_data: str):
    #     df = pd.read_json(json_data)

    #     pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')

    #     insert_query = """
    #         INSERT INTO table_created (
    #             order_id,
    #             order_date,
    #             customer_id,
    #             region,
    #             product,
    #             quantity,
    #             unit_price,
    #             total_amount
    #         )
    #         VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    #         ON CONFLICT (order_id) DO NOTHING
    #     """

    #     # Convert DataFrame → list of tuples
    #     data_params = [
    #         (
    #             row['order_id'],
    #             row['order_date'],
    #             row['customer_id'],
    #             row['region'],
    #             row['product'],
    #             row['quantity'],
    #             row['unit_price'],
    #             row['total_amount']
    #         )
    #         for _, row in df.iterrows()
    #     ]

    #     # Single call (NO loop here)
    #     pg_hook.run(insert_query, parameters=data_params)

    @task()
    def load_data(json_data: str):
        df = pd.read_json(StringIO(json_data))

        pg_hook = PostgresHook(postgres_conn_id='analytics_postgres')

        rows = [
            ( # tuple is here !! because opostgres understands tuples not dataframes 
                row['order_id'],
                row['order_date'],
                row['customer_id'],
                row['region'],
                row['product'],
                row['quantity'],
                row['unit_price'],
                row['total_amount']
            )
            for _, row in df.iterrows()
             #_ skip indexes and iterrows is a function of pandas that allows you to go one row at a time
        ]

        pg_hook.insert_rows(
            table='sales',
            rows=rows,
            target_fields=[
                'order_id',
                'order_date',
                'customer_id',
                'region',
                'product',
                'quantity',
                'unit_price',
                'total_amount'
            ]
        )

    # -----------------------------------
    # DAG Dependencies
    # -----------------------------------
    table = create_sales_table()
    csv_data = extract_from_s3()
    transformed_data = transform_data(csv_data)
    table >> csv_data >> transformed_data >> load_data(transformed_data)
