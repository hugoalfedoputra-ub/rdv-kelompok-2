import os
from dotenv import load_dotenv

import requests
import json
from kafka import KafkaProducer

load_dotenv()

HOURLY_TOPIC = "hourly"
REAL_TIME_TOPIC = "real-time"

OPEN_WEATHER_KEY = os.getenv("OPEN_WEATHER_KEY")
FREE_WEATHER_KEY = os.getenv("FREE_WEATHER_KEY")


class WeatherProducer:
    def __init__(self, lat, lon, server_addr="localhost:29092"):
        self.open_weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPEN_WEATHER_KEY}"
        self.open_meteo_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,apparent_temperature,relative_humidity_2m,rain,precipitation,weather_code,cloud_cover,showers,wind_speed_10m,wind_direction_10m,pressure_msl,surface_pressure,wind_gusts_10m&timezone=Asia%2FBangkok&forecast_days=1"
        self.free_weather_url = f"https://api.weatherapi.com/v1/current.json?key={FREE_WEATHER_KEY}&q={lat},{lon}&aqi=no"

        self.producer = KafkaProducer(
            bootstrap_servers=server_addr,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def get_open_weather(self):
        """
        Ideally GET every 15 minutes
        """
        try:
            res = requests.get(self.open_weather_url)
            if res.status_code == 200:
                data = res.json()
            else:
                print(res.status_code)
        except Exception as e:
            print(e)
            return None
        return data

    def get_open_meteo(self):
        """
        Ideally GET at every chance (but new data is pushed every 15 minutes).
        Based on API limit, you can GET up to 3 requests per minute.
        """
        try:
            res = requests.get(self.open_meteo_url)
            if res.status_code == 200:
                data = res.json()
            else:
                print(res.status_code)
        except Exception as e:
            print(e)
            return None
        return data

    def get_free_weather(self):
        """
        Ideally GET at every chance (but new data is pushed every 15 minutes).
        Based on API limit, you can GET up to 22 requests per minute.
        """
        try:
            res = requests.get(self.free_weather_url)
            if res.status_code == 200:
                data = res.json()
            else:
                print(res.status_code)
        except Exception as e:
            print(e)
            return None
        return data
