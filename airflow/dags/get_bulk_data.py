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
PROVIDER_NAME = os.getenv("OPEN_METEO_PROVIDER")
