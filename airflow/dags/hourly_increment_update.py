import os
import couchdb
import sqlite3
import logging
import requests
from datetime import datetime
from airflow.sdk import Variable
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

SQLITE_PATH = Variable.get("SQLITE_PATH")
# Couchdb variables
COUCHDB_HOST = Variable.get("COUCHDB_HOST")
COUCH_PORT = Variable.get("COUCH_PORT")
COUCHDB_USERNAME = Variable.get("COUCHDB_USERNAME")
COUCHDB_PASSWORD = Variable.get("COUCHDB_PASSWORD")
AGGREGATE_DB = Variable.get("AGGREGATE_DB")
COUCHDB_URI = f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCH_PORT}"

server = couchdb.Server(COUCHDB_URI)
db = server[AGGREGATE_DB]

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s]:%(message)s"
)

def flatten_dict(d) -> dict:
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

def fetch_data() -> dict:
    date_now = datetime.now().strftime("%Y-%m-%d")
    query = {
        "selector" :{
            "timestamp" :{
                "$gte" : date_now
            }
        },
        "sort" : [{"timestamp" : "desc"}],
        "limit":1
    }
    logging.info("Fetching recent data...")
    temp_list = []
    try:
        rows = db.find(query)
    except Exception as e:
        logging.error(f"Error fetching recent data, message : {e}")

    for row in rows:
        doc = flatten_dict(row)
        temp_list.append(doc)
    
    return temp_list

def increment_update():
    recent_data = fetch_data()
    recent_timestamp = recent_data.get("timestamp")
    recent_timestamp = recent_timestamp.strptime(recent_timestamp, "%Y-%m-%dT%H:%M:%S")
    parsed_timestamp = recent_data.strftime(recent_timestamp, "%Y-%m-%d %H:%M")
    recent_data["timestamp"] = parsed_timestamp

    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO weather_summary (location_name, latitude, longitude, timestamp, cloud_total_pct, wind_speed_kmph, pressure, humidity_pct, temperature_c, feels_like_c, wind_gust_kmph)
        VALUES (:location_name, :latitude, :longitude, :timestamp, :avg_cloud_total_pct, :avg_wind_speed_kmph, :avg_pressure, :avg_humidity_pct, :avg_temperature_c, :avg_feels_like_c, :avg_wind_gust_kmph)
    """, recent_data[0])
    
    conn.commit()
    conn.close()

with DAG(
    dag_id="hourly_increment_update",
    start_date=datetime(2025, 6, 8),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["weather", "sql", "hourly"]
) as dag:
    increment_update_task = PythonOperator(
        task_id="perform_hourly_incremental_update",
        python_callable=increment_update
)
