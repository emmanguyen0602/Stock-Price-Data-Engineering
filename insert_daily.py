from datetime import date, datetime, timedelta
import requests
import pandas as pd
import time
import json
import os
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# Set google configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dags/sublime-iridium-388409-572842960a48.json'

# Set default settings
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'scheduled_interval':'0 8 * * *' # Run every day at 08:00 AM
}
dag = DAG(
    'stock_price_dag_daily',
    default_args=default_args,
    catchup=False,
    tags=['Insert daily data']
)

def craw_stock_price():
    today = date.today()
    stock_code = "DIG"
    
    print("Chuẩn bị crawl {}".format(today))
    # Initialize an empty DataFrame outside the loop for better performance
    stock_price_df = pd.DataFrame()

    url = "https://finfo-api.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{}~date:{}".format(stock_code, today)
    print(url)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0'}
    x = requests.get(url, timeout=10, headers=headers)
    # Use the json() method directly on the response object to parse the JSON response
    json_x = x.json()['data']

    for stock in json_x:
        # Append the data to the DataFrame inside the loop:
        stock_price_df = stock_price_df.append(stock, ignore_index=True)

    time.sleep(5)
    stock_price_df.to_csv("dags/stock_price_daily.csv", index=None)
    # Convert DataFrame to a JSON-serializable format
    stock_price_json = stock_price_df.to_json(orient='records')
    return stock_price_json

# Process the stock price data
def process_stock_price(**kwargs):
    ti = kwargs['ti']
    stock_price_json = ti.xcom_pull(task_ids='craw_stock_price')
def load_to_bigquery():
    # Read the stock price CSV file
    stock_price_df = pd.read_csv("dags/stock_price_daily.csv")

    # Create a BigQuery client
    bigquery_client = bigquery.Client()

    # Get the existing dataset
    dataset_ref = bigquery_client.dataset("Stock_VNDIRECT")

    # Define the table reference
    table_ref = dataset_ref.table("stock_prices")

    # Append the DataFrame to the table
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

    job = bigquery_client.load_table_from_dataframe(stock_price_df, table_ref, job_config=job_config)
    job.result()  # Wait for the job to complete

    print("Data appended to BigQuery table: {}.{}".format(dataset_ref.dataset_id, table_ref.table_id))

with dag:
    crawl_task = PythonOperator(
        task_id='craw_stock_price',
        python_callable=craw_stock_price,
    )
    process_task = PythonOperator(
        task_id='process_stock_price',
        python_callable=process_stock_price
    )
    load_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery,
    )

    crawl_task >> process_task >> load_task





