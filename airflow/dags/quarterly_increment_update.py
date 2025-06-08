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
FREEWEATHER_DB = Variable.get("FREEWEATHER_DB")
COUCHDB_URI = f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCH_PORT}"

server = couchdb.Server(COUCHDB_URI)
db = server[FREEWEATHER_DB]

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
            "created_at" :{
                "$gte" : date_now
            }
        },
        "sort" : [{"created_at" : "desc"}],
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
    datenow = datetime.now().strftime("%Y-%m-%d %H:%M")

    recent_data = fetch_data()
    recent_data["timestamp"] = datenow

    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO weather_data (
            provider, name, region, country, lat, lon, tz_id,
            localtime_epoch, localtime, last_updated_epoch, last_updated,

            temp_c, temp_f, is_day, text, icon, code,
            wind_mph, wind_kph, wind_degree, wind_dir,
            pressure_mb, pressure_in, precip_mm, precip_in,
            humidity, cloud, feelslike_c, feelslike_f,
            windchill_c, windchill_f, heatindex_c, heatindex_f,
            dewpoint_c, dewpoint_f, vis_km, vis_miles, uv,
            gust_mph, gust_kph
        )
        VALUES (
            :provider, :name, :region, :country, :lat, :lon, :tz_id,
            :localtime_epoch, :localtime, :last_updated_epoch, :last_updated,

            :temp_c, :temp_f, :is_day, :text, :icon, :code,
            :wind_mph, :wind_kph, :wind_degree, :wind_dir,
            :pressure_mb, :pressure_in, :precip_mm, :precip_in,
            :humidity, :cloud, :feelslike_c, :feelslike_f,
            :windchill_c, :windchill_f, :heatindex_c, :heatindex_f,
            :dewpoint_c, :dewpoint_f, :vis_km, :vis_miles, :uv,
            :gust_mph, :gust_kph
        )
    """, recent_data[0])
    
    conn.commit()
    conn.close()

with DAG(
    dag_id="quarterly_increment_update",
    start_date=datetime(2025, 6, 8),
    schedule="0 * * * *",
    catchup=False,
    tags=["weather", "sql", "quarterly"]
) as dag:
    increment_update_task = PythonOperator(
        task_id="perform_quarterly_incremental_update",
        python_callable=increment_update
)