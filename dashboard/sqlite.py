import sqlite3
from datetime import datetime
import pandas as pd

class sqliteModel:
    def __init__(self, file_path):
        self.file_path = file_path

    def _is_near_certain_time(self, dt, time_filter, tolerance=2):
        if time_filter=="hourly":
            filter = [0]
        elif time_filter=="quarter":
            filter = [0, 15, 30, 45]
        else :
            print(f"There's no filter named {time_filter}")

        minutes = dt.minute
        for base in filter:
            if abs(minutes - base) <= tolerance:
                return True
        return False

    def _filter_certain_time(self, data, key="created_at",  time_filter="hourly", tolerance=2):
        result = []
        for item in data:
            try:
                dt = datetime.strptime(item[key], "%Y-%m-%d %H:%M")
                if self._is_near_certain_time(dt, tolerance=tolerance, time_filter=time_filter):
                    result.append(item)
            except Exception as e:
                print(f"Format waktu salah: {item.get(key)}")
        return result
    
    def get_all_hourly_temperature(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, avg_temperature_c FROM weather_summary", conn)
        results = df.to_dict(orient="list")
        return results
    
    def get_all_quarter_temperature(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT localtime, temp_c FROM weather_data", conn)
        results = df.to_dict(orient="list")
        return results
    
    def get_all_hourly_humidity(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, avg_humidity_pct FROM weather_summary", conn)
        results = df.to_dict(orient="list")
        return results
    
    def get_all_quarter_humidity(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT localtime, humidity FROM weather_data", conn)
        results = df.to_dict(orient="list")
        return results
    
    def get_hourly_precipitation(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT localtime, precip_mm FROM weather_data", conn)
        df = df.rename(columns={"localtime" : "timestamp"})
        unfiltered = df.to_dict(orient="records")
        filtered = pd.DataFrame(self._filter_certain_time(unfiltered))
        return filtered
    
    def get_quarter_precipitation(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT localtime, precip_mm FROM weather_data", conn)
        results = df.to_dict(orient="list")
        return results

    def get_hourly_icon(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT localtime, icon FROM weather_data", conn)
        df = df.rename(columns={"localtime" : "timestamp"})
        unfiltered = df.to_dict(orient="records")
        filtered = pd.DataFrame(self._filter_certain_time(unfiltered))
        return filtered
    
    def get_quarter_icon(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT icon FROM weather_data", conn)
        results = df.to_dict(orient="list")
        return results