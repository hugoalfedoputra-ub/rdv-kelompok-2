import os
import json
import logging
from dotenv import load_dotenv
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

load_dotenv()

HOURLY_TOPIC = "hourly"
REAL_TIME_TOPIC = "real-time"

OPEN_WEATHER_KEY = os.getenv("OPEN_WEATHER_KEY")
FREE_WEATHER_KEY = os.getenv("FREE_WEATHER_KEY")


class WeatherProducer:
    def __init__(self, lat, lon, server_addr="localhost:29092"):
        if not OPEN_WEATHER_KEY:
            log.warning("OPEN_WEATHER_KEY environment variable not set.")
        if not FREE_WEATHER_KEY:
            log.warning("FREE_WEATHER_KEY environment variable not set.")

        self.lat = lat
        self.lon = lon

        self.open_weather_url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={OPEN_WEATHER_KEY}"
        self.open_meteo_url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&current=temperature_2m,apparent_temperature,relative_humidity_2m,rain,precipitation,weather_code,cloud_cover,showers,wind_speed_10m,wind_direction_10m,pressure_msl,surface_pressure,wind_gusts_10m&timezone=Asia%2FBangkok&forecast_days=1"
        self.free_weather_url = f"https://api.weatherapi.com/v1/current.json?key={FREE_WEATHER_KEY}&q={lat},{lon}&aqi=no"

        self.producer = None
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=server_addr,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=3,
            )
            log.info(f"KafkaProducer connected to {server_addr}")
        except KafkaError as e:
            log.error(f"Failed to initialize Kafka Producer: {e}")

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

    def _make_request(self, url, provider_name):
        """Helper function to make HTTP requests"""

        log.info(f"Fetching data from {provider_name} API...")
        try:
            res = requests.get(url, timeout=30)
            res.raise_for_status()
            data = res.json()
            log.info(f"Successfully fetched data from {provider_name}.")
            return data
        except requests.exceptions.Timeout:
            log.error(f"Request timed out for {provider_name} API ({url})")
        except requests.exceptions.HTTPError as http_err:
            log.error(
                f"HTTP error occurred for {provider_name}: {http_err} - Status Code: {res.status_code}, Response: {res.text[:200]}..."
            )
        except requests.exceptions.RequestException as req_err:
            log.error(f"Error during request to {provider_name}: {req_err}")
        except Exception as e:
            log.error(
                f"An unexpected error occurred fetching from {provider_name}: {e}",
                exc_info=True,
            )
        return None

    def get_open_weather(self):
        """
        Ideally GET every 15 minutes
        """
        if not OPEN_WEATHER_KEY:
            log.error("Cannot fetch OpenWeatherMap data: API key not configured.")
            return None
        return self._make_request(self.open_weather_url, "OpenWeatherMap")

    def get_open_meteo(self):
        """
        Ideally GET at every chance (but new data is pushed every 15 minutes).
        Based on API limit, you can GET up to 3 requests per minute.
        """
        return self._make_request(self.open_meteo_url, "Open-Meteo")

    def get_free_weather(self):
        """
        Ideally GET at every chance (but new data is pushed every 15 minutes).
        Based on API limit, you can GET up to 22 requests per minute.
        """
        if not FREE_WEATHER_KEY:
            log.error("Cannot fetch FreeWeatherAPI data: API key not configured.")
            return None
        return self._make_request(self.free_weather_url, "FreeWeatherAPI")

    def send_to_kafka(self, topic, data, provider_name):
        """Sends data to the specified Kafka topic."""
        if not self.producer:
            log.error("Kafka producer not initialized. Cannot send message.")
            return False
        if data is None:
            log.warning(f"No data provided to send for {provider_name}.")
            return False

        try:
            data_to_send = {
                "provider": provider_name,
                "latitude": self.lat,
                "longitude": self.lon,
                "data": data,
            }
            _ = self.producer.send(topic, value=data_to_send)
            log.info(
                f"Message for {provider_name} queued successfully for topic '{topic}'."
            )
            return True
        except KafkaError as e:
            log.error(
                f"Failed to send message for {provider_name} to Kafka topic {topic}: {e}"
            )
            return False
        except Exception as e:
            log.error(
                f"An unexpected error occurred during Kafka send for {provider_name}: {e}",
                exc_info=True,
            )
            return False

    def close(self):
        if self.producer:
            log.info("Closing Kafka producer...")
            self.producer.flush()
            self.producer.close()
            log.info("Kafka producer closed.")
