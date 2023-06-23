
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

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 6, 22),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def craw_stock_price():
    to_date = date.today()
    from_date = to_date - timedelta(days=60)

    to_date = to_date.strftime("%Y-%m-%d")
    from_date = from_date.strftime("%Y-%m-%d")
    stock_code = "DIG"
    
    print("Chuẩn bị crawl {} {}".format(from_date, to_date))
    #Initialize an empty DataFrame outside the loop for better performance
    stock_price_df = pd.DataFrame()

    url = "https://finfo-api.vndirect.com.vn/v4/stock_prices?sort=date&q=code:{}~date:gte:{}~date:lte:{}&size=9990&page=1".format(stock_code, from_date, to_date)
    print(url)
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:12.0) Gecko/20100101 Firefox/12.0'}
    x = requests.get(url, timeout=10, headers=headers)
    #Use the json() method directly on the response object to parse the JSON response
    json_x = x.json()['data']

    for stock in json_x:
        #Append the data to the DataFrame inside the loop:
        stock_price_df = stock_price_df.append(stock, ignore_index=True)

    time.sleep(5)
    stock_price_df.to_csv("dags/stock_price.csv", index=None)
    # Convert DataFrame to a JSON-serializable format
    stock_price_json = stock_price_df.to_json(orient='records')
    return stock_price_json

# Process the stock price data
def process_stock_price(**kwargs):
    ti = kwargs['ti']
    stock_price_json = ti.xcom_pull(task_ids='craw_stock_price')

def load_to_google_storage():

    from google.cloud import storage

    # Create a storage client
    storage_client = storage.Client()

    try:
        # Create a Cloud Storage Bucket
        storage_client.create_bucket(bucket_or_name='stock_emmanguyen0602')
    except Exception:
        # Grab the bucket
        bucket = storage_client.get_bucket(bucket_or_name='stock_emmanguyen0602')

    # Create blob
    blob = bucket.blob('stock_price.csv')

    # Upload to bucket
    blob.upload_from_filename('dags/stock_price.csv')

def load_to_bigquery() -> None:

    # Fetch the DataFrame
    stock_price = pd.read_csv('dags/stock_price.csv')

    # Create a BigQuery client
    bigquery_client = bigquery.Client()

    # Get or Create a data set 
    try:
        # Create the dataset
        dataset = bigquery_client.create_dataset('Stock_VNDIRECT')
    except Exception:
        # Grab the data set "Stock_VNDIRECT"
        dataset = bigquery_client.get_dataset('Stock_VNDIRECT')

    # Define table details
    table = dataset.table('stock_prices')
    table.schema = [
        #code,date,time,floor,type,basicPrice,ceilingPrice,floorPrice,open,high,low,close,average,adOpen,adHigh,adLow,adClose,adAverage,nmVolume,nmValue,ptVolume,ptValue,change,adChange,pctChange
        bigquery.SchemaField('code', 'TEXT', mode='REQUIRED'),
        bigquery.SchemaField('date', 'DATE', mode='REQUIRED'),
        bigquery.SchemaField('time', 'TIME', mode='REQUIRED'),
        bigquery.SchemaField('floor', 'TEXT', mode='REQUIRED'),
        bigquery.SchemaField('type', 'TEXT', mode='REQUIRED'),
        bigquery.SchemaField('basicPrice', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('ceilingPrice', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('floorPrice', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('open', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('high', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('low', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('close', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('average', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('adOpen', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('adHigh', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('adLow', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('adClose', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('adAverage', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('nmVolume', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('nmValue', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('ptVolume', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('ptValue', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('change', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('adChange', 'DECIMAL', mode='REQUIRED'),
        bigquery.SchemaField('pctChange', 'DECIMAL', mode='REQUIRED')]

    # Get or Create a table
    try:
        # Create the table
        table = bigquery_client.create_table(table)
    except Exception:
        # Grab the table
        table = bigquery_client.get_table(table)

    # Load the DataFrame stock_price to the table stock_prices
    bigquery_client.load_table_from_dataframe(dataframe=stock_price, destination=table)

with DAG('stock_price_dag', default_args=default_args, schedule_interval='@daily') as dag:
    crawl_task = PythonOperator(
        task_id='crawl_stock_price',
        python_callable=craw_stock_price
    )

    process_task = PythonOperator(
        task_id='process_stock_price',
        python_callable=process_stock_price
    )

    google_storage_task = PythonOperator(
        task_id='load_to_google_storage',
        python_callable=load_to_google_storage
    )

    bigquery_task = PythonOperator(
        task_id='load_to_bigquery',
        python_callable=load_to_bigquery
    )

    crawl_task >> process_task >> [google_storage_task, bigquery_task]