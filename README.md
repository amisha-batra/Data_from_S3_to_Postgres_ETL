**S3 to PostgreSQL ETL Pipeline Using Apache Airflow**

This project implements an end-to-end ETL (Extract, Transform, Load) pipeline using Apache Airflow (Astro runtime) to move data from Amazon S3 into a PostgreSQL database. The pipeline is designed using Airflow’s TaskFlow API and follows a clear, modular structure that mirrors real-world data engineering practices.

DAG Overview and Scheduling

The pipeline is defined as an Airflow DAG scheduled to run daily with catchup disabled, ensuring that only current data is processed. The DAG orchestrates multiple tasks with explicit dependencies, guaranteeing that each step executes in the correct order.

Table Creation and Database Preparation

The first task in the pipeline ensures that the target PostgreSQL table exists before any data is loaded. Using Airflow’s PostgresHook, a CREATE TABLE IF NOT EXISTS statement is executed, making the task idempotent. This prevents failures during data loading and allows the pipeline to be safely re-run multiple times.

Data Extraction from Amazon S3

The extract task connects to Amazon S3 using Airflow’s S3Hook and reads a CSV file from a specified bucket. The file is loaded entirely as a string, representing the raw CSV content. This string is returned by the task and stored in Airflow’s XCom system, which allows downstream tasks to access the extracted data.

Data Transformation Using Pandas

The transform task receives the CSV string from XCom and converts it into a Pandas DataFrame using StringIO. This enables file-like reading of in-memory strings. Data cleaning and transformation steps are then applied, including filtering invalid records, computing derived metrics such as total transaction amount, removing duplicates, and handling missing numeric values using mean imputation. After transformation, the DataFrame is serialized into a JSON string using to_json(orient='records'), making it safe to pass between Airflow tasks.

Data Loading into PostgreSQL

The load task receives the transformed JSON string from XCom and reconstructs the Pandas DataFrame. Since PostgreSQL does not accept DataFrames directly, each row of the DataFrame is converted into a tuple, with values ordered to match the database schema. These tuples are collected into a list and inserted into PostgreSQL in bulk using PostgresHook’s insert_rows method, ensuring efficient and reliable data loading.

Data Flow and XCom Usage

Data flows through the pipeline in multiple representations: as a CSV file in S3, a CSV string in Airflow, a DataFrame during transformation, a JSON string stored in XCom, and finally as structured rows in PostgreSQL. XCom acts as the communication layer between tasks, enabling safe and serialized data transfer across task boundaries.

Key Engineering Practices Demonstrated

This pipeline demonstrates important data engineering best practices, including separation of extract, transform, and load responsibilities, idempotent database operations, safe data serialization, bulk database inserts, and clean DAG dependency management. The design is scalable, maintainable, and suitable for production-style ETL workflows.
