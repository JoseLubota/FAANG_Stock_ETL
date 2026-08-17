import findspark
import pandas as pd
import yfinance as yf
from pyspark.sql.window import Window
import logging
import os
import sys
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta

# Adding scripts directory's parent to path so 'scripts' resolves as a package
SCRIPTS_PATH = '/opt/airflow'
if SCRIPTS_PATH not in sys.path:
    sys.path.insert(0, SCRIPTS_PATH)
    
logger = logging.getLogger(__name__)

# Trying to import PySpark
try:
    import pyspark
    logger.info(f'Pyspark imported, version: {pyspark.__version__}')
except ImportError as e:
    logger.error(f'Failed to import PySpark: {e}')

# Import ETL function from scripts
try:
    from scripts.stock_etl import run_stock_etl
    logger.info('ETL function imported succesfully!') 
except ImportError as e:
    etl_import_error = str(e)
    logger.error(f'Failed to import ETL function {e}')
    
    def run_stock_etl(**kwargs):
        logger.error(f'ETL function not available. Error: {etl_import_error}')
        return {'status': 'error', 'message':f'ETL function not available: {etl_import_error}'}
#_______________________________________________________

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date':datetime(2026,8,14),
    'retries':2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure':False,
    'email_on_retry':False,
    'catchup': False
}

dag = DAG(
    'stock_etl_dag',
    default_args=default_args,
    description='ETL pipeline for stock data from FAANG companies using yfinance and PySpark',
    schedule='0 8 * * *',
    tags=['stock', 'ETL', 'yfinance', 'pyspark', 'postgresql']
)

# Task to run the ETL process for stock data    
run_task = PythonOperator(
    task_id='run_stock_etl',
    python_callable=run_stock_etl,
    op_kwargs={
        'ticker': ['AMZN', 'AAPL', 'GOOGL', 'META', 'NFLX'],
        'period':'20y',
        'jdbc_url':'jdbc:postgresql://postgres:5432/airflow',
        'user':'admin',
        'password':'airflow',
        },
    dag=dag
)

# Task to verify if data has been loaded into the PostgreSQL database
verify_load_task = SQLExecuteQueryOperator(
    task_id='verify_load_task',
    conn_id='postgres_default',
    sql="""
    SELECT
            COUNT(*) as total_records,
            COUNT(DISTINCT company) as companies,
            MIN(date) as earliest_date,
            MAX(date) as latest_date
    FROM stock_etl
    """,
    dag=dag
)

# Task to clean up old logs
cleanup_task = EmptyOperator(
    task_id='cleanup_task',
    dag=dag
)
# Set Task dependencies
run_task >> verify_load_task >> cleanup_task
logger.info('DAG loaded sucessfully!')