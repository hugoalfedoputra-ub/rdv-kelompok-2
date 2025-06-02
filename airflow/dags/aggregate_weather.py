import tempfile
import os
import logging
import couchdb
import json
from airflow.sdk import Variable
from airflow.models.dag import DAG
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql.functions import avg, col, to_timestamp, when, minute, lit, date_format
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from utils.weather_aggregator import Aggregator
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

# Initialize Spark session (akan digunakan di dalam function)
def get_spark_session():
    try:
        # Try Spark Connect first with proper container networking
        logging.info(f"Attempting to connect to Spark Connect at: {PYSPARK_WORKER_URI}")
        spark = SparkSession.builder \
            .remote(PYSPARK_WORKER_URI) \
            .config("spark.sql.connect.grpc.deadline", "60s") \
            .config("spark.sql.connect.grpc.maxInboundMessageSize", "134217728") \
            .getOrCreate()
        
        # Test connection
        spark.sql("SELECT 1 as test").collect()
        logging.info("Spark Connect connection successful!")
        return spark
        
    except Exception as e:
        logging.warning(f"Spark Connect failed: {e}")
        logging.warning("Falling back to local Spark mode...")
        # Fallback to local mode
        return SparkSession.builder \
            .appName("WeatherAggregation") \
            .master("local[*]") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()

# Initialize CouchDB server
server = couchdb.Server(f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCH_PORT}")
db_name_list = [FREEWEATHER_DB, OPENMETEO_DB, OPENWEATHER_DB]

def flatten_and_normalize(d, target_keys=None):
    flat = {}
    for k, v in d.items():
        if isinstance(v, dict):
            flat.update(flatten_and_normalize(v, target_keys))
        else:
            # Normalisasi nilai numerik jika perlu
            if target_keys is None or k in target_keys:
                try:
                    if v is not None and isinstance(v, (int, float, str)):
                        flat[k] = float(v)
                    else:
                        flat[k] = v
                except (ValueError, TypeError):
                    flat[k] = None
            else:
                flat[k] = v
    return flat


def fetch_data():
    spark = get_spark_session()
    
    freeweather_psdf = None
    openmeteo_psdf = None
    openweather_psdf = None

    for db_name in db_name_list:
        temp_list = []
        db = server[db_name]
        rows = db.view("_all_docs", include_docs=True)

        for row in rows:
            doc = row["doc"]
            doc = flatten_and_normalize(doc)
            temp_list.append(doc)

        # Convert list to JSON strings untuk createDataFrame
        if temp_list:
            # Method 1: Menggunakan createDataFrame langsung dengan data Python
            try:
                if db_name == FREEWEATHER_DB:
                    freeweather_psdf = spark.createDataFrame(temp_list)
                elif db_name == OPENMETEO_DB:
                    openmeteo_psdf = spark.createDataFrame(temp_list)
                elif db_name == OPENWEATHER_DB:
                    openweather_psdf = spark.createDataFrame(temp_list)
            except Exception as e:
                # Method 2: Jika Method 1 gagal, coba dengan parallelize (local mode)
                logging.warning(f"CreateDataFrame failed, trying parallelize method: {e}")
                try:
                    json_strings = [json.dumps(item) for item in temp_list]
                    df = spark.read.json(create_temp_json_df(spark, json_strings))

                    if db_name == FREEWEATHER_DB:
                        freeweather_psdf = df
                    elif db_name == OPENMETEO_DB:
                        openmeteo_psdf = df
                    elif db_name == OPENWEATHER_DB:
                        openweather_psdf = df

                except Exception as e2:
                    # Method 3: Temporary file approach
                    logging.warning(f"Parallelize also failed, using temp file method: {e2}")
                    json_strings = [json.dumps(item) for item in temp_list]
                    
                    if db_name == FREEWEATHER_DB:
                        freeweather_psdf = create_temp_json_df(spark, json_strings)
                    elif db_name == OPENMETEO_DB:
                        openmeteo_psdf = create_temp_json_df(spark, json_strings)
                    elif db_name == OPENWEATHER_DB:
                        openweather_psdf = create_temp_json_df(spark, json_strings)
    
    return freeweather_psdf, openmeteo_psdf, openweather_psdf

def create_temp_json_df(spark, json_strings):
    """
    Alternative method untuk membuat DataFrame dari JSON strings
    ketika sparkContext tidak tersedia (Spark Connect)
    """
    
    # Buat temporary file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as temp_file:
        for json_str in json_strings:
            temp_file.write(json_str + '\n')
        temp_file_path = temp_file.name
    
    try:
        # Baca dari temporary file
        df = spark.read.json(f"file://{temp_file_path}")
        return df
    finally:
        # Hapus temporary file
        os.unlink(temp_file_path)

def aggregate(data_list: list):
    processed_psdf = []
    aggregator = Aggregator()

    for i, data in enumerate(data_list):
        if data is None:
            continue
            
        # Standarisasi Nama Kolom
        data = aggregator.standardize_columns(data, col_map=None)
        data = data.withColumnRenamed("last_updated", "date")
        if "name" in data.columns:
            data = data.withColumnRenamed("name", "location_name")
        else :
            data = data.withColumn("location_name", lit("Malang"))

        logging.info(f"Data No.{i+1} has successfully renamed, with column: {data.columns}")

        # Pengecekan skala kolom temperatur
        temp_avg_row = data.select(avg("temperature_c")).collect()[0]
        temp_avg_value = temp_avg_row[0] if temp_avg_row[0] is not None else 0
        
        if temp_avg_value > 273.15:
            data = data.withColumn("temperature_c", col("temperature_c") - 273.15)
        
        # Pemformatan data tanggal untuk filtering
        if "date" in data.columns:
            data = data.withColumn(
                "timestamp",
                when(
                    col("date").contains("+"),
                    to_timestamp("date")
                ).otherwise(
                    when(
                        col("date").contains("T"),
                        to_timestamp("date", "yyyy-MM-dd'T'HH:mm")
                    ).otherwise(
                        to_timestamp("date", "yyyy-MM-dd HH:mm")
                    )
                )
            )
        else:
            logging.warning(f"Date Column not found on data no. {i+1}")
        
        # Mengambil data per jam (ketika menit == 0)
        data = data.filter(minute(col("timestamp")) == 0)

        # Parsing agar semua tanggal (dengan 3 format berbeda) menjadi 1 format ISO yg seragam
        data = data.withColumn("date", date_format("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
        
        # Drop columns dengan handling untuk kolom yang mungkin tidak ada
        for col_name in ["_id", "_rev", "provider", "created_at"]:
            if col_name in data.columns:
                data = data.drop(col_name)
        
        data = data.orderBy("timestamp")
        processed_psdf.append(data)
    
    if processed_psdf:
        processed_psdf = aggregator.aggregate_common_columns(
            dfs=processed_psdf, 
            group_cols=["location", "latitude", "longitude", "timestamp", "date"]
        )
    else:
        # Return empty DataFrame jika tidak ada data
        spark = get_spark_session()
        processed_psdf = spark.sql("SELECT 1 as dummy").limit(0)
    
    return processed_psdf

def perform_pipeline():
    try:
        freeweather_psdf, openmeteo_psdf, openweather_psdf = fetch_data()
        processed_psdf = aggregate([freeweather_psdf, openmeteo_psdf, openweather_psdf])
        
        if processed_psdf is not None and processed_psdf.count() > 0:
            db = server[AGGREGATE_DB]
            
            # Collect data dan save ke CouchDB
            collected_data = processed_psdf.collect()
            for row in collected_data:
                doc = row.asDict()
                # Convert any non-serializable types
                for key, value in doc.items():
                    if value is None:
                        continue
                    # Handle datetime/timestamp objects
                    if hasattr(value, 'isoformat'):
                        doc[key] = value.isoformat()
                
                try:
                    db.save(doc)
                except Exception as e:
                    logging.error(f"Error saving document: {e}")
                    continue
        
        # Stop Spark session
        spark = get_spark_session()
        spark.stop()
        
    except Exception as e:
        logging.error(f"Pipeline error: {e}")
        # Ensure Spark session is stopped even on error
        try:
            spark = get_spark_session()
            spark.stop()
        except:
            pass
        raise

# DAG Definition
with DAG(
    dag_id="aggregate_from_three_source",
    description="Aggregate data from FreeWeather, OpenMeteo, and OpenWeather to CouchDB",
    start_date=datetime(2025, 5, 1),
    schedule="0 0 * * *",
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