import os
import json
import requests
import tempfile
import logging
import traceback
import couchdb
import logging
from airflow.sdk import Variable
from airflow.models.dag import DAG
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from utils.weather_aggregator import Aggregator
from utils.schemas import WeatherSchemas
from pyspark.sql.utils import AnalysisException

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s]:%(message)s"
)

# PySpark variables
PYSPARK_WORKER_URI = Variable.get("PYSPARK_WORKER_URI")

# Couchdb variables
COUCHDB_HOST = Variable.get("COUCHDB_HOST")
COUCH_PORT = Variable.get("COUCH_PORT")
COUCHDB_USERNAME = Variable.get("COUCHDB_USERNAME")
COUCHDB_PASSWORD = Variable.get("COUCHDB_PASSWORD")
FREEWEATHER_DB = Variable.get("FREEWEATHER_DB")
OPENMETEO_DB = Variable.get("OPENMETEO_DB")
OPENWEATHER_DB = Variable.get("OPENWEATHER_DB")
AGGREGATE_DB = Variable.get("AGGREGATE_DB")
COUCHDB_URI = f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCH_PORT}"

server = couchdb.Server(COUCHDB_URI)
db_name_list = [FREEWEATHER_DB, OPENMETEO_DB, OPENWEATHER_DB]

logging.basicConfig(
    level=logging.DEBUG,  # Ubah ke DEBUG supaya semua log tampil
    format="[%(levelname)s] %(asctime)s - %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def get_spark_session(max_retries=3):
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempting to connect to Spark Connect at: {PYSPARK_WORKER_URI}")
            spark = SparkSession.builder \
                .remote(PYSPARK_WORKER_URI) \
                .config("spark.sql.connect.grpc.deadline", "60s") \
                .config("spark.sql.connect.grpc.maxInboundMessageSize", "134217728") \
                .config("spark.sql.connect.grpc.keepAliveTimeout", "5s") \
                .config("spark.sql.connect.grpc.keepAliveTime", "30s") \
                .config("spark.sql.connect.grpc.keepAliveWithoutCalls", "true") \
                .config("spark.sql.connect.grpc.maxConnecionIdle", "900s") \
                .getOrCreate()
            spark.sql("SELECT 1 as test").collect()
            logger.info("Spark Connect connection successful!")
            return spark
        except Exception as e:
            logger.warning(f"Spark Connect failed: {e}")
            logger.warning("Falling back to local Spark mode...")
            spark = SparkSession.builder \
                .appName("WeatherAggregation") \
                .master("local[*]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            logger.info("Local Spark session started.")
            return spark

def flatten_dict(d):
    flat = {}
    for k, v in d.items():
        if isinstance(v, dict):
            flat.update(flatten_dict(v))
        elif isinstance(v, list):
            if v and isinstance(v[0], dict):
                for item in v:
                    flat.update(flatten_dict(item))
            else:
                flat[k] = v
        else:
            flat[k] = v
    return flat

def create_temp_json_df(spark, json_strings):
    logger.debug("Creating temp JSON file for DataFrame creation")
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        for json_str in json_strings:
            temp_file.write(json_str + '\n')
        temp_file_path = temp_file.name
    
    try:
        df = spark.read.json(f"file://{temp_file_path}")
        logger.debug(f"Temporary JSON DataFrame created from file: {temp_file_path}")
        return df
    finally:
        try:
            os.unlink(temp_file_path)
            logger.debug(f"Temporary JSON file deleted: {temp_file_path}")
        except Exception as e:
            logger.warning(f"Failed to delete temporary file: {e}")


def fetch_data(spark=None):
    logger.info("Starting fetch_data process...")
    if spark is None:
        spark = get_spark_session()
    
    freeweather_psdf = None
    openmeteo_psdf = None
    openweather_psdf = None

    for db_name in db_name_list:
        index_def = {
            "index": {"fields": ["created_at"]},
            "name": "timestamp_index",
            "type": "json"
        }
        index_url = f"{COUCHDB_URI}/{FREEWEATHER_DB}/_index"
        resp = requests.post(index_url, json=index_def)
        logger.info("Index creation:", resp.status_code, resp.text)

        one_hour_ago = datetime.utcnow() - timedelta(hours=2)
        one_hour_ago_str = one_hour_ago.strftime("%Y-%m-%d %H:%M:%S.%f")

        query = {
            "selector": {
                "created_at": {"$gte": one_hour_ago_str}
            },
            "sort": [{"created_at": "desc"}],
            "limit": 100
        }

        logger.info(f"Fetching data from database: {db_name}")
        temp_list = []
        db = server[db_name]
        try:
            rows = db.find(query)
        except Exception as e:
            logger.error(f"Error fetching rows from {db_name}: {e}")
            continue

        for row in rows:
            doc = row.get("doc", {})
            doc = flatten_dict(doc)
            temp_list.append(doc)

        if not temp_list:
            logger.warning(f"No documents found in {db_name}")
            continue

        try:
            if db_name == FREEWEATHER_DB:
                freeweather_psdf = spark.createDataFrame(temp_list, WeatherSchemas.freeweather_schema)
                # Force evaluation and cache
                freeweather_psdf = freeweather_psdf.cache()
                row_count = freeweather_psdf.count()
                logger.info(f"Created freeweather DataFrame: columns={len(freeweather_psdf.columns)}, rows={row_count}")
            elif db_name == OPENMETEO_DB:
                openmeteo_psdf = spark.createDataFrame(temp_list, WeatherSchemas.open_meteo_schema)
                # Force evaluation and cache
                openmeteo_psdf = openmeteo_psdf.cache()
                row_count = openmeteo_psdf.count()
                logger.info(f"Created openmeteo DataFrame: columns={len(openmeteo_psdf.columns)}, rows={row_count}")
            elif db_name == OPENWEATHER_DB:
                openweather_psdf = spark.createDataFrame(temp_list, WeatherSchemas.openweather_schema)
                # Force evaluation and cache
                openweather_psdf = openweather_psdf.cache()
                row_count = openweather_psdf.count()
                logger.info(f"Created openweather DataFrame: columns={len(openweather_psdf.columns)}, rows={row_count}")
        except Exception as e:
            logger.warning(f"CreateDataFrame failed for {db_name}: {e}")
            try:
                json_strings = [json.dumps(item) for item in temp_list]
                df = spark.read.json(create_temp_json_df(spark, json_strings))
                df = df.cache()  # Cache the fallback DataFrame too
                if db_name == FREEWEATHER_DB:
                    freeweather_psdf = df
                elif db_name == OPENMETEO_DB:
                    openmeteo_psdf = df
                elif db_name == OPENWEATHER_DB:
                    openweather_psdf = df
                logger.info(f"Created DataFrame using parallelize/read.json for {db_name} with {df.count()} rows")
            except Exception as e2:
                logger.warning(f"Parallelize method failed for {db_name}: {e2}")
                try:
                    json_strings = [json.dumps(item) for item in temp_list]
                    df = create_temp_json_df(spark, json_strings)
                    df = df.cache()  # Cache the temp file DataFrame too
                    if db_name == FREEWEATHER_DB:
                        freeweather_psdf = df
                    elif db_name == OPENMETEO_DB:
                        openmeteo_psdf = df
                    elif db_name == OPENWEATHER_DB:
                        openweather_psdf = df
                    logger.info(f"Created DataFrame using temp file method for {db_name} with {df.count()} rows")
                except Exception as e3:
                    logger.error(f"All DataFrame creation methods failed for {db_name}: {e3}")
    
    # Debug: Log the final counts
    logger.info(f"Final fetch results:")
    logger.info(f"  FreeWeather: {freeweather_psdf.count() if freeweather_psdf else 0} rows")
    logger.info(f"  OpenMeteo: {openmeteo_psdf.count() if openmeteo_psdf else 0} rows")
    logger.info(f"  OpenWeather: {openweather_psdf.count() if openweather_psdf else 0} rows")
    
    return freeweather_psdf, openmeteo_psdf, openweather_psdf, spark


def aggregate(data_list: list, spark=None):
    logger.info(f"Starting aggregation with {len(data_list)} dataframes")
    if spark is None:
        spark = get_spark_session()
    
    processed_psdf = []
    aggregator = Aggregator(column_map=None)

    # Debug: Check input data
    for i, data in enumerate(data_list):
        logger.info(f"Input DataFrame No.{i+1}: {data is not None} - {data.count() if data else 0} rows")

    for i, data in enumerate(data_list):
        if data is None:
            logger.warning(f"DataFrame at index {i} is None, skipping aggregation")
            continue
        
        logger.info(f"Processing DataFrame No.{i+1} with {data.count()} rows")
        logger.info(f"DataFrame No.{i+1} columns before standardize: {data.columns}")
        
        try:
            data = aggregator.standardize_columns(data, col_map=None)
            if "name" in data.columns:
                data = data.withColumnRenamed("name", "location_name")
            else:
                data = data.withColumn("location_name", lit("Malang"))
            
            logger.info(f"DataFrame No.{i+1} columns after rename/add 'location_name': {data.columns} with {data.count()} rows")

            temp_avg_row = data.select(avg("temperature_c")).collect()[0]
            temp_avg_value = temp_avg_row[0] if temp_avg_row[0] is not None else 0
            logger.debug(f"Average temperature_c for DataFrame No.{i+1}: {temp_avg_value}")

            if temp_avg_value > 273.15:
                logger.info(f"Adjusting temperature_c from Kelvin to Celsius for DataFrame No.{i+1}")
                data = data.withColumn("temperature_c", col("temperature_c") - 273.15)

            data = data.withColumnRenamed("last_updated", "date")

            if "date" in data.columns:
                data = data.withColumn(
                    "timestamp",
                    when(
                        col("date").contains("+"),
                        to_timestamp("date", "yyyy-MM-dd HH:mm:ssXXX")
                    ).otherwise(
                        when(
                            col("date").contains("T"),
                            to_timestamp("date", "yyyy-MM-dd'T'HH:mm")
                        ).otherwise(
                            to_timestamp("date", "yyyy-MM-dd HH:mm")
                        )
                    )
                )
                logger.debug(f"Timestamp column created for DataFrame No.{i+1}")
            else:
                logger.warning(f"Date column not found in DataFrame No.{i+1}")

            data = data.withColumn("date", date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
            data = data.withColumn("hour_timestamp", date_trunc("hour", col("timestamp")))

            group_cols = ["location_name", "latitude", "longitude", "hour_timestamp"]
            value_cols = [c for c in data.columns if c not in group_cols + ["date", "timestamp"]]
            logger.debug(f"Group columns: {group_cols}")
            logger.debug(f"Value columns for aggregation: {value_cols}")

            logger.info(f"Before groupBy aggregation: {data.count()} rows")
            agg_exprs = [avg(c).alias(c) for c in value_cols]
            data = data.groupBy(*group_cols).agg(*agg_exprs)
            logger.info(f"After groupBy aggregation: {data.count()} rows")

            data = data.withColumn("date", date_format("hour_timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
            data = data.withColumnRenamed("hour_timestamp", "timestamp")

            logger.info(f"DataFrame No.{i+1} aggregated with {len(data.columns)} columns and {data.count()} rows")

            for col_name in ["_id", "_rev", "provider", "created_at"]:
                if col_name in data.columns:
                    data = data.drop(col_name)
                    logger.debug(f"Dropped column {col_name} from DataFrame No.{i+1}")

            data = data.orderBy("timestamp")
            
            # Cache the processed DataFrame
            data = data.cache()
            processed_psdf.append(data)
            
        except Exception as e:
            logger.error(f"Error processing DataFrame No.{i+1}: {e}")
            logger.error(traceback.format_exc())
            continue
    
    logger.info(f"Successfully processed {len(processed_psdf)} DataFrames")
    
    if processed_psdf:
        logger.info(f"Before final aggregation: {len(processed_psdf)} DataFrames")
        
        # Debug: Show counts before final aggregation
        for i, df in enumerate(processed_psdf):
            logger.info(f"  DataFrame {i+1}: {df.count()} rows")
        
        final_result = aggregator.aggregate_common_columns(
            dfs=processed_psdf, 
            group_cols=["location_name", "latitude", "longitude", "timestamp"]
        )
        
        # Cache final result and force evaluation
        final_result = final_result.cache()
        final_count = final_result.count()
        
        logger.info(f"Aggregated common columns across all DataFrames successfully: {final_count} rows")
        return final_result
    else:
        logger.warning("No DataFrames to aggregate, returning empty DataFrame")
        empty_df = spark.sql("SELECT 1 as dummy").limit(0)
        return empty_df


def perform_pipeline():
    logger.info("Pipeline started")
    spark = None
    
    try:
        # Step 1: Initialize Spark session
        spark = get_spark_session()
        spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        
        # Step 2: Fetch data with explicit spark session
        freeweather_psdf, openmeteo_psdf, openweather_psdf, spark = fetch_data(spark)
        
        # Step 3: Debug - Check data fetching results
        logger.info(f"Fetched data verification:")
        logger.info(f"  FreeWeather: {freeweather_psdf.count() if freeweather_psdf else 0} rows")
        logger.info(f"  OpenMeteo: {openmeteo_psdf.count() if openmeteo_psdf else 0} rows")
        logger.info(f"  OpenWeather: {openweather_psdf.count() if openweather_psdf else 0} rows")
        
        # Step 4: Show sample data for debugging
        for i, (name, df) in enumerate([("FreeWeather", freeweather_psdf), 
                                       ("OpenMeteo", openmeteo_psdf), 
                                       ("OpenWeather", openweather_psdf)]):
            if df is not None:
                logger.info(f"{name} DataFrame sample (first 2 rows):")
                try:
                    df.show(2, truncate=False)
                except Exception as e:
                    logger.warning(f"Could not show {name} sample: {e}")
        
        # Step 5: Aggregate data
        processed_psdf = aggregate([freeweather_psdf, openmeteo_psdf, openweather_psdf], spark)
        processed_psdf = processed_psdf.orderBy("timestamp", ascending=False)

        if processed_psdf is not None:
            count_rows = processed_psdf.count()
            logger.info(f"Data successfully aggregated with {len(processed_psdf.columns)} columns and {count_rows} rows")
            
            # Show final result sample
            logger.info("Final aggregated data sample:")
            try:
                processed_psdf.show(5, truncate=False)
            except Exception as e:
                logger.warning(f"Could not show final sample: {e}")
        else:
            logger.warning("No aggregated data returned")

        # Step 6: Save to CouchDB
        if processed_psdf is not None and processed_psdf.count() > 0:
            db = server[AGGREGATE_DB]
            collected_data = processed_psdf.collect()
            logger.info(f"Saving {len(collected_data)} documents to CouchDB: {AGGREGATE_DB}")

            for i, row in enumerate(collected_data):
                doc = row.asDict()
                for key, value in doc.items():
                    if value is None:
                        continue
                    if hasattr(value, 'isoformat'):
                        doc[key] = value.isoformat()
                try:
                    db.save(doc)
                    logger.debug(f"Document {i+1} saved")
                except Exception as e:
                    logger.error(f"Error saving document {i+1}: {e}")
        else:
            logger.warning("No data to save to CouchDB")

        logger.info("Pipeline completed successfully")

    except Exception as e:
        logger.error(f"Pipeline error: {e}")
        logger.error(traceback.format_exc())
        raise
    finally:
        # Step 7: Clean up Spark session
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped successfully")
            except Exception as e2:
                logger.error(f"Failed to stop Spark session: {e2}")

# DAG Definition
with DAG(
    dag_id="aggregate_from_three_source",
    description="Aggregate data from FreeWeather, OpenMeteo, and OpenWeather to CouchDB",
    start_date=datetime(2025, 5, 1),
    schedule="  ",
    catchup=False,
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5)
    },
    tags=["weather", "aggregation", "couchdb"]
) as dag:
    aggregate_task = PythonOperator(
        task_id="perform_weather_aggregation",
        python_callable=perform_pipeline
    )