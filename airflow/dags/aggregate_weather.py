import couchdb
from airflow.sdk import Variable
from airflow.models.dag import DAG
from pyspark.sql import SparkSession
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from pyspark.sql.functions import avg, col, to_timestamp, when, minute, lit
from utils.weather_aggregator import Aggregator

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

spark = SparkSession.builder.remote(PYSPARK_WORKER_URI).getOrCreate()
server = couchdb.Server(f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCH_PORT}")
db_name_list = [FREEWEATHER_DB, OPENMETEO_DB, OPENWEATHER_DB]

def flatten_dict(d):
    flat = {}
    for v in d.values():
        if isinstance(v, dict):
            flat.update(flatten_dict(v))
    for k, v in d.items():
        if not isinstance(v, dict):
            flat[k] = v
    
    return flat

def fetch_data():
    freeweather_psdf = None
    openmeteo_psdf = None
    openweather_psdf = None

    for db_name in db_name_list:
        temp_list = []
        db = server[db_name]
        rows = db.view("_all_docs", include_docs=True)

        for row in rows:
            doc = row["doc"]
            doc = flatten_dict(doc)
            temp_list.append(doc)

        if db_name == FREEWEATHER_DB:
            freeweather_psdf = spark.read.json(spark.sparkContext.parallelize(temp_list))
        elif db_name == OPENMETEO_DB:
            openmeteo_psdf = spark.read.json(spark.sparkContext.parallelize(temp_list))
        elif db_name == OPENWEATHER_DB:
            openweather_psdf = spark.read.json(spark.sparkContext.parallelize(temp_list))
    
    return freeweather_psdf, openmeteo_psdf, openweather_psdf

def aggregate(data_list:list):
    processed_psdf = []
    aggregator = Aggregator()

    for data in data_list:
        # Standarisasi Nama Kolom
        data = aggregator.standardize_columns(data)
        # Pengecekan skala kolom temperatur
        temp_avg = data.select(avg("temperature_c"))
        if temp_avg > 273.15:
            data = data.withColumn("temperature_c", col("temperature_c") - 273.15)
            data = data.withColumnRenamed("last_updated", "date")
            data = data.withColumn("location", lit("Malang"))
        data = data.withColumnRenamed("name", "location")
        # Pemformatan data tanggal untuk filtering
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
        # Mengambil data per jam (ketika menit == 0)
        data = data.filter(minute(col("timestamp"))==0)
        data = data.drop("_id", "_rev", "provider", "created_at", "last_updated")
        data = data.orderBy("timestamp")

        processed_psdf.append(data)
    
    processed_psdf = aggregator.aggregate_common_columns(dfs=processed_psdf, group_cols=["location", "latitude", "longitude", "timestamp", "date"])
    
    return processed_psdf

def perform_pipeline():
    freeweather_psdf, openmeteo_psdf, openweather_psdf = fetch_data()
    processed_psdf = aggregate([freeweather_psdf, openmeteo_psdf, openweather_psdf])
    db = server[AGGREGATE_DB]

    for row in processed_psdf.collect():
        doc = row.asDict()
        db.save(doc)

with DAG(
    dag_id="aggregate_from_three_source",
    description="Aggregate data from FreeWeather, OpenMeteo, and OpenWeather to CouchDB",
    start_date=datetime(2025, 5, 1),
    schedule_interval="0 0 * * *",
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
