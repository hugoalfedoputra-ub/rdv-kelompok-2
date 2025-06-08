import os
import requests
import couchdb
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

COUCHDB_HOST = os.getenv("COUCHDB_HOST")
COUCH_PORT = os.getenv("COUCH_PORT")
COUCHDB_USERNAME = os.getenv("COUCHDB_USERNAME")
COUCHDB_PASSWORD = os.getenv("COUCHDB_PASSWORD")
FREEWEATHER_DB = os.getenv("FREEWEATHER_DB")
OPENMETEO_DB = os.getenv("OPENMETEO_DB")
OPENWEATHER_DB = os.getenv("OPENWEATHER_DB")
AGGREGATE_DB = os.getenv("AGGREGATE_DB")
COUCHDB_URI = f"http://{COUCHDB_USERNAME}:{COUCHDB_PASSWORD}@{COUCHDB_HOST}:{COUCH_PORT}"

server = couchdb.Server(COUCHDB_URI)
db_name_list = [FREEWEATHER_DB, OPENMETEO_DB, OPENWEATHER_DB]

class CouchModel:
    def flatten_dict(self, d):
        flat = {}
        for k, v in d.items():
            if isinstance(v, dict):
                flat.update(self.flatten_dict(v))
            elif isinstance(v, list):
                if v and isinstance(v[0], dict):
                    for item in v:
                        flat.update(self.flatten_dict(item))
                else:
                    flat[k] = v
            else:
                flat[k] = v
        return flat
    
    def get_aggregate(self):
        now = datetime.now() \
                .replace(minute=0, second=0, microsecond=0) \
                .strftime("%Y-%m-%d %H:%M:%S.%f")
        
        index_def = {
            "index": {"fields": ["timestamp"]},
            "name": "timestamp_index",
            "type": "json"
        }
        index_url = f"{COUCHDB_URI}/{AGGREGATE_DB}/_index"
        _ = requests.post(index_url, json=index_def)

        query = {
            "selector": {
                "timestamp": {"$lte": now}
            },
            "sort": [{"timestamp": "desc"}],
            "limit": 100
        }

        temp_list = []
        db = server[AGGREGATE_DB]
        try:
            rows = db.find(query)
        except Exception as e:
            print(e)
        
        for row in rows:
            doc = self.flatten_dict(row)
            temp_list.append(doc)
        
        return temp_list
        
    def get_freeweather(self):
        hournowutc = datetime.utcnow().replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")
        index_def = {
        "index": {"fields": ["created_at"]},
        "name": "timestamp_index",
        "type": "json"
        }
        index_url = f"{COUCHDB_URI}/{FREEWEATHER_DB}/_index"
        _ = requests.post(index_url, json=index_def)

        query = {
            "selector": {
                "created_at": {"$lte": hournowutc}
            },
            "sort": [{"created_at": "desc"}],
            "limit": 100000
        }

        temp_list = []
        db = server[FREEWEATHER_DB]
        try:
            rows = db.find(query)
        except Exception as e:
            print(e)

        for row in rows:
            doc = self.flatten_dict(row)
            temp_list.append(doc)
        return temp_list

    def get_openmeteo(self):
        nowutc = datetime.utcnow().replace(minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S.%f")
        index_def = {
        "index": {"fields": ["created_at"]},
        "name": "timestamp_index",
        "type": "json"
        }
        index_url = f"{COUCHDB_URI}/{OPENMETEO_DB}/_index"
        _ = requests.post(index_url, json=index_def)

        query = {
            "selector": {
                "created_at": {
                    "$lte": nowutc,
                    }
            },
            "sort": [{"created_at": "desc"}],
            "limit": 32
        }

        temp_list = []
        db = server[OPENMETEO_DB]
        try:
            rows = db.find(query)
        except Exception as e:
            print(e)

        for row in rows:
            doc = self.flatten_dict(row)
            temp_list.append(doc)
        return temp_list

