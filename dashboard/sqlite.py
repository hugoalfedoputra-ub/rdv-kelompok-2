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

    def _filter_certain_time(self, data, key="created_at",  time_filter:str="hourly", tolerance=2):
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
        df = pd.read_sql_query("SELECT timestamp, temperature_c FROM weather_summary", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_all_quarter_temperature(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, temp_c FROM weather_data", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df
    
    def get_all_hourly_feelslike(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, feels_like_c FROM weather_summary", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_all_quarter_feelslike(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, feelslike_c FROM weather_data", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_all_hourly_humidity(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, humidity_pct FROM weather_summary", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_all_quarter_humidity(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, humidity FROM weather_data", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_hourly_precipitation(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, precip_mm FROM weather_data", conn)
        unfiltered = df.to_dict(orient="records")
        filtered = pd.DataFrame(self._filter_certain_time(unfiltered, key="timestamp"))
        filtered = filtered.sort_values("timestamp")
        return filtered
    
    def get_quarter_precipitation(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, precip_mm FROM weather_data", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_hourly_icon(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, icon FROM weather_data", conn)
        unfiltered = df.to_dict(orient="records")
        filtered = pd.DataFrame(self._filter_certain_time(unfiltered, key="timestamp", time_filter="hourly"))
        filtered["timestamp"] = pd.to_datetime(filtered["timestamp"])
        filtered = filtered.sort_values("timestamp")
        return filtered
    
    def get_quarter_icon(self):
        conn = sqlite3.connect(self.file_path)
        df = pd.read_sql_query("SELECT timestamp, icon FROM weather_data", conn)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df = df.sort_values("timestamp")
        return df

    def get_recent_hourly_weather(self):
        conn = sqlite3.connect(self.file_path)
        query = """
        SELECT 
            temperature_c AS temp_c,
            wind_speed_kmph AS wind_kph,
            pressure,
            humidity_pct AS humidity,
            feels_like_c AS feelslike_c,
            wind_gust_kmph AS gust_kph,
            cloud_total_pct AS cloud
        FROM weather_summary
        ORDER BY timestamp DESC
        LIMIT 1
        """
        df = pd.read_sql_query(query, conn)
        results = df.to_dict(orient="records")
        return results[0]

    def get_recent_quarterly_weather(self):
        conn = sqlite3.connect(self.file_path)
        query = """
        SELECT 
            temp_c,
            wind_kph,
            pressure_mb AS pressure,
            humidity,
            feelslike_c,
            gust_kph,
            cloud
        FROM weather_data
        ORDER BY timestamp DESC
        LIMIT 1
        """
        df = pd.read_sql_query(query, conn)
        results = df.to_dict(orient="records")
        return results[0]