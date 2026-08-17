import findspark
findspark.init()
import pyspark
import pandas as pd
import yfinance as yf
from pyspark.sql.functions import col, round, when, lag, avg, first, stddev, min as spark_min, max as spark_max
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
import logging
import os

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_spark_session():
    # Create and return spark session with PostgreSQL
    spark = ( SparkSession.builder
            .master("local[*]")
            .appName('stock_etl_airflow')
            .config('spark.jars.packages','org.postgresql:postgresql:42.7.12')
            .config('spark.sql.adaptive.enabled', 'true')
            .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
            .config('spark.serializer', 'org.apache.spark.serializer.KryoSerializer')
            .getOrCreate()
        )
    # Setting log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    return spark

def fetch_stock_data(ticker, period='20y'):
    # Fetch stock data from Yahoo Finance
    logger.info(f'Fetching data for {ticker} for the last {period}')
    try:
        #Download data
        df = yf.download(ticker, period=period, group_by='ticker')
        
        if df.empty:
            raise ValueError(f"No data found for ticker {ticker}")
        
        #----------------------------------------------------------------------------------
        # Convert to DataFrame
        df = (df.stack(level=0).rename_axis(['date','company']).reset_index())
        # Rename columns to lowercase
        df.columns = df.columns.str.lower()
        # Map Company to real names
        company_map = {
        "META":	'meta',
        "GOOGL": 'google',
        "AMZN":	'amazon',
        "AAPL":	'apple',
        "NFLX":	'netflix'
        }
        df['company'] = df['company'].map(company_map)
        logger.info(f'Sucessfully fetched {len(df)} records.')
        return df
    except Exception as e: 
        logger.error(f"Error fetching data for {ticker}: {e}")
        raise
        #----------------------------------------------------------------------------------

def transform_data(spark, df):
    # Transform the data using PySpark
    logger.info('Starting data transformation')
    # Create Spark DataFrame
    sdf = spark.createDataFrame(df)
    #Rename columns
    sdf = (
        sdf
            .withColumnRenamed('open', 'open_price')
            .withColumnRenamed('high', 'high_price')
            .withColumnRenamed('low', 'low_price')
            .withColumnRenamed('date', 'date')
            .withColumnRenamed('dividends', 'dividends')
            .withColumnRenamed('stock splits', 'stock_splits')
            .withColumnRenamed('close', 'close_price')
            .withColumnRenamed('volume', 'volume')
    )
    # Filering invalid data
    sdf = sdf.filter(col('close_price') > 0)
    # Calculating the daily return percentage
    window = Window.orderBy("date")
    sdf = sdf.withColumn('previous_close', lag('close_price').over(window))
    sdf = sdf.withColumn('daily_return_pct', round(((col('close_price') - col('previous_close')) / col('previous_close')) * 100,2))

    # Calculating Daily based metrics
    # Calculating the daily price range 
    sdf = sdf.withColumn("daily_range",round(col('high_price') - col('low_price'), 2))
    # Daily range percentage
    sdf = sdf.withColumn('daily_range_pct', round(((col('high_price') - col('low_price')) / col('low_price')) *100, 2))
    # Gap between opening anf previous close
    sdf = sdf.withColumn('gap_pct', round(((col('open_price') - col('previous_close')) / col('previous_close')) *100, 2))
    
    # Calculatign Rolling Statistics

    # 7-Day and 14-day Rolling Averages
    sdf = sdf.withColumn('mov_avg_14d', round(avg('close_price').over(Window.partitionBy('company').orderBy('date').rowsBetween(-13,0)),2))

    # Rolling Volatility (Standard Deviation of Daily Returns)
    sdf = sdf.withColumn('volatility_7d', round(stddev('daily_return_pct').over(Window.partitionBy('company').orderBy('date').rowsBetween(-6,0)),2))

    # Volume-based metrics
    # Average volume over the last 7 days
    sdf = sdf.withColumn('avg_volume_7d', round(avg('volume').over(Window.partitionBy('company').orderBy('date').rowsBetween(-6,0)), 2))
    # Average volume ratio
    sdf = sdf.withColumn('volume_ratio', round(col('volume') / col('avg_volume_7d'), 2))

    # Relative Performance
    sdf = sdf.withColumn('sector_avg_return', avg('daily_return_pct').over(Window.partitionBy('date')))
    sdf = sdf.withColumn('relative_performance', round(col('daily_return_pct') - col('sector_avg_return'), 2))

    # Cumulative Return
    window_by_company = Window.partitionBy('company').orderBy('date')
    sdf = sdf.withColumn('first_price', first('close_price').over(window_by_company))

    sdf = sdf.withColumn('cumulative_return', round(((col('close_price') - col('first_price')) / col('first_price')) * 100, 2))
    sdf = sdf.drop('first_price')
    
    logger.info('Data transformation completed successfully')
    return sdf

def load_to_postgres(sdf, jdbc_url, user, password, table_name='stock_etl'):
    # Load the transformed data to PostgreSQL
    logger.info(f'Loadin data to PostGreSQL table {table_name}')
    
    properties = {
        "user":user,
        "password":password,
        "driver":"org.postgresql.Driver"
    }
    
    try: 
        sdf.write.jdbc(
        url=jdbc_url,
        table=table_name,
        mode='append',
        properties=properties
        )
        logger.info(f'Succesfully loaded {sdf.count()} records to table {table_name}')
    except Exception as e:
        logger.error(f'Error loading data to PostgreSQL: {str(e)}')
        raise
    
def run_stock_etl(ticker, period, jdbc_url, user, password, **context):
    # Main ETL function to be called by Airflow
    logger.info('Starting ETL process')
    try:
        # Get Spark Session
        spark = get_spark_session()
        # Extract
        df = fetch_stock_data(ticker, period)
        # Transform
        sdf = transform_data(spark, df)
        # Load
        load_to_postgres(sdf, jdbc_url, user, password)
        
        # Log the number of records loaded
        record_count = sdf.count()
        logger.info(f'ETF completed succesfully. Loaded {record_count} records for {ticker}')
        
        # Return a summary for Airflow's XCom
        return {
            'ticker': ticker,
            'records_loaded': record_count,
            'start_date': sdf.select(spark_min('date')).collect()[0][0],
            'end_date': sdf.select(spark_max('date')).collect()[0][0],
        }
    except Exception as e:
        logger.error(f'ETL process failed: {str(e)}')
        raise
    finally:
        if 'spark' in locals():
            spark.stop()
            logger.info('Spark session stopped')