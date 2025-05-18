from __future__ import annotations
from datetime import timedelta

import pendulum
import logging
import os
from dotenv import load_dotenv

load_dotenv()

from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

from utils.weather_producer import WeatherProducer


log = logging.getLogger(__name__)

LATITUDE = -7.95
LONGITUDE = 112.61
KAFKA_TOPIC = "free-weather"
PROVIDER_NAME = os.getenv("FREE_WEATHER_PROVIDER")


def fetch_and_send_data():

    producer = None

    try:
        log.info(f"Initializing WeatherProducer for {PROVIDER_NAME}...")
        producer = WeatherProducer(
            lat=LATITUDE, lon=LONGITUDE, server_addr="broker-1:19092"
        )
        if not producer.producer:
            raise RuntimeError(
                "Failed to initialize Kafka Producer in WeatherProducer."
            )

        data = producer.get_free_weather()

        if data:
            log.info(
                f"Fetched data from {PROVIDER_NAME}. Sending to Kafka topic '{KAFKA_TOPIC}'..."
            )
            success = producer.send_to_kafka(KAFKA_TOPIC, data, PROVIDER_NAME)
            if not success:
                raise RuntimeError(f"Failed to send data for {PROVIDER_NAME} to Kafka.")
            else:
                log.info(f"Successfully sent data from {PROVIDER_NAME} to Kafka.")
        else:
            log.warning(f"No data received from {PROVIDER_NAME}.")

    except Exception as e:
        log.error(
            f"Error in fetch_and_send_data for {PROVIDER_NAME}: {e}", exc_info=True
        )
        raise
    finally:
        if producer:
            producer.close()


with DAG(
    dag_id="free_weather_fetch_to_kafka",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=timedelta(seconds=20),
    catchup=False,
    tags=["weather", "kafka", "free-weather"],
    doc_md="""
    ### Free Weather API Data Fetch DAG
    Fetches current weather data from api.weatherapi.com frequently
    and sends it to the 'hourly' Kafka topic.
    Requires `FREE_WEATHER_KEY` environment variable.
    """,
) as dag:
    fetch_free_weather_task = PythonOperator(
        task_id="fetch_and_send_free_weather",
        python_callable=fetch_and_send_data,
    )
