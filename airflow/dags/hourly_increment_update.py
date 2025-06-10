import os
import couchdb
import sqlite3
import logging
import requests
from datetime import datetime
from airflow.sdk import Variable
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator
from zoneinfo import ZoneInfo

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

def fetch_data() -> list:
    date_now = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d")
    query = {
        "selector" :{
            "created_at" :{
                "$gte" : date_now
            }
        },
        "sort" : [{"created_at" : "desc"}],
        "limit":1
    }
    logging.info(f"Fetching recent data with query: {query}")
    logging.info(f"Current date filter: {date_now}")
    
    temp_list = []
    try:
        rows = db.find(query)
        row_count = 0
        for row in rows:
            row_count += 1
            logging.info(f"Found row {row_count}: {row.get('created_at', 'No created_at field')}")
            doc = flatten_dict(row)
            temp_list.append(doc)
        
        logging.info(f"Total rows processed: {row_count}")
        
        # If no data found for today, try to get the latest record
        if not temp_list:
            logging.info("No data found for today, fetching latest record...")
            fallback_query = {
                "sort" : [{"created_at" : "desc"}],
                "limit": 1
            }
            rows = db.find(fallback_query)
            for row in rows:
                logging.info(f"Found fallback row: {row.get('created_at', 'No created_at field')}")
                doc = flatten_dict(row)
                temp_list.append(doc)
                
    except Exception as e:
        logging.error(f"Error fetching recent data, message : {e}")
        return []
    
    logging.info(f"Returning {len(temp_list)} records")
    return temp_list

def increment_update():
    datenow = datetime.now(ZoneInfo("Asia/Jakarta")).strftime("%Y-%m-%d %H:%M")

    recent_data_list = fetch_data()
    
    # Debug logging
    logging.info(f"Fetched data list length: {len(recent_data_list)}")
    
    # Check if data exists
    if not recent_data_list:
        logging.warning("No recent data found to insert")
        return
    
    # Get the first (and only) record from the list
    recent_data = recent_data_list[0]
    
    logging.info(f"Processing data record with keys: {list(recent_data.keys())}")

    conn = sqlite3.connect(SQLITE_PATH)
    cursor = conn.cursor()
    
    try:
        # Map CouchDB fields to SQLite table fields for weather_summary
        summary_data = {
            'location_name': recent_data.get('name', ''),
            'latitude': recent_data.get('lat', 0.0),
            'longitude': recent_data.get('lon', 0.0),
            'timestamp': datenow,
            'cloud_total_pct': recent_data.get('cloud', 0.0),
            'wind_speed_kmph': recent_data.get('wind_kph', 0.0),
            'pressure': recent_data.get('pressure_mb', 0.0),
            'humidity_pct': recent_data.get('humidity', 0.0),
            'temperature_c': recent_data.get('temp_c', 0.0),
            'feels_like_c': recent_data.get('feelslike_c', 0.0),
            'wind_gust_kmph': recent_data.get('gust_kph', 0.0)
        }
        
        logging.info(f"Mapped summary data: {summary_data}")
        
        cursor.execute("""
            INSERT OR REPLACE INTO weather_summary (
                location_name, latitude, longitude, timestamp,
                cloud_total_pct, wind_speed_kmph, pressure, humidity_pct,
                temperature_c, feels_like_c, wind_gust_kmph
            )
            VALUES (
                :location_name, :latitude, :longitude, :timestamp,
                :cloud_total_pct, :wind_speed_kmph, :pressure, :humidity_pct,
                :temperature_c, :feels_like_c, :wind_gust_kmph
            )
        """, summary_data)
        
        conn.commit()
        logging.info("Data successfully inserted into weather_summary table")
        
    except Exception as e:
        logging.error(f"Error inserting data into SQLite: {e}")
        logging.error(f"Data that failed to insert: {summary_data}")
        conn.rollback()
    finally:
        conn.close()

with DAG(
    dag_id="hourly_weather_summary_update",
    start_date=datetime(2025, 6, 8),
    schedule="0 * * * *",  # Every hour
    catchup=False,
    tags=["weather", "sql", "hourly", "summary"]
) as dag:
    increment_update_task = PythonOperator(
        task_id="perform_hourly_summary_update",
        python_callable=increment_update
)