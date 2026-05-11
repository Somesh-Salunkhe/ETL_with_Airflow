"""
ETL Pipeline
"""

# Imports
from airflow import DAG
from airflow.providers.http.operators.http import HttpOperator
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import json


# Define DAG
with DAG(
    dag_id = "nasa_apod_postgres",
    start_date = datetime(2026, 5, 10),
    schedule = "@daily",
    catchup = False
) as dag:
    # Step 1: Create table

    @task
    def create_table():
        # Initialize PostgresHook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        # SQL Query to create a table
        create_table_query = """
        CREATE TABLE IF NOT EXISTS apod_data (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255),
            explanation TEXT,
            url TEXT,
            date DATE,
            media_type VARCHAR(50)
        );
        """ 

        # Execute the query
        postgres_hook.run(create_table_query)


    # Step 2: Extract NASA API Data (APOD) - Astronomy Picture of the Day
    
    extract_apod = HttpOperator(
        task_id = "extract_apod",
        http_conn_id  = "nasa_api",                       # Connection ID defined in Airflow for NASA API
        endpoint = 'planetary/apod',                      # NASA API Endpoint for APOD
        method = 'GET', 
        data={
        "api_key": "{{ conn.nasa_api.extra_dejson.api_key }}"},  # Using API Key from connection
        response_filter = lambda response: response.json(),   # Convert response to json
    )

    # Step 3: Transform Data 

    @task
    def transform_data(response):
        '''
        We will make use of get() method to extract required information from response. The first argument
        in get() is the required parameter ex. "get('title', '')", this extracts the required field, here
        title. Next parameter states that if the required field information is absent, mark it as a blank
        when get method is called.
        '''
        apod_data = {
            'title' : response.get('title', ''),       
            'explanation' : response.get('explanation', ''),
            'url' : response.get('url', ''),
            'date' : response.get('date', ''),
            'media_type' : response.get('media_type', '')
        }
        return apod_data

    # Step 4: Load data into Postgres SQL

    @task
    def load_data(apod_data):
        # Initialize  the postgres hook
        postgres_hook = PostgresHook(postgres_conn_id = "my_postgres_connection")

        # Define SQL INSERT Query
        insert_query = """
        INSERT INTO apod_data (title, explanation, url, date, media_type)
        VALUES (%s, %s, %s, %s, %s);
        """

        # Execute query
        postgres_hook.run(insert_query, parameters = (
            apod_data['title'],
            apod_data['explanation'],
            apod_data['url'],
            apod_data['date'],
            apod_data['media_type']
        ))

    # Step 5: Verify data with DBeaver

    # Step 6: Define task dependencies

    ## Extract
    create_table() >> extract_apod  # Ensure table is created before extraction
    api_response = extract_apod.output

    ## Transform
    transformed_data = transform_data(api_response)
    
    ## Load
    load_data(transformed_data)
