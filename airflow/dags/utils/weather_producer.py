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

OPEN_WEATHER_PROVIDER = os.getenv("OPEN_WEATHER_PROVIDER")
OPEN_METEO_PROVIDER = os.getenv("OPEN_METEO_PROVIDER")
FREE_WEATHER_PROVIDER = os.getenv("FREE_WEATHER_PROVIDER")


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
        return self._make_request(self.open_weather_url, OPEN_WEATHER_PROVIDER)

    def get_open_meteo(self):
        """
        Ideally GET at every chance (but new data is pushed every 15 minutes).
        Based on API limit, you can GET up to 3 requests per minute.
        """
        return self._make_request(self.open_meteo_url, OPEN_METEO_PROVIDER)

    def get_free_weather(self):
        """
        Ideally GET at every chance (but new data is pushed every 15 minutes).
        Based on API limit, you can GET up to 22 requests per minute.
        """
        if not FREE_WEATHER_KEY:
            log.error("Cannot fetch FreeWeatherAPI data: API key not configured.")
            return None
        return self._make_request(self.free_weather_url, FREE_WEATHER_PROVIDER)

    def get_meteo_bulk(self, start_date: str, end_date: str):
        """
        GET bulk historical data, best if only run once and manually
        """
        url = f"https://archive-api.open-meteo.com/v1/archive?latitude={self.lat}&longitude={self.lon}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,is_day,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,et0_fao_evapotranspiration,vapour_pressure_deficit,wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=auto"

        if (start_date != "") and (end_date != ""):
            if int(end_date.split("-")[0]) - int(start_date.split("-")[0]) > 1:
                print(
                    "WARNING: YOU ARE GOING TO REQUEST MORE THAN ONE YEAR'S WORTH OF DATA FROM THE API.\n\tTHIS WILL COST YOU A LOT OF API CALLS (UPWARDS OF HUNDREDS).\n\tPLEASE BE SURE YOU ARE NOT RUNNING THIS MULTIPLE TIMES AS IT WILL DEPLETE YOUR DAILY LIMIT!"
                )
            return self._make_request(url, OPEN_METEO_PROVIDER)
        else:
            print("Start date and/or end date parameter is not supplied.")
            return None

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
