import os
import traceback
import threading
import time
from dotenv import load_dotenv

# Assuming etl_kafka.py is in the same directory or Python path
from utils.kafka_to_db import (
    KafkaETL,
)  # Make sure this import works based on your file structure

import pendulum
import logging
from airflow.models.dag import DAG
from airflow.providers.standard.operators.python import PythonOperator

load_dotenv()

log = logging.getLogger(__name__)

COUCHDB_UNAME = os.getenv("APP_USER")
COUCHDB_PWD = os.getenv("APP_PASSWORD")
COUCHDB_HOST = os.getenv("COUCHDB_HOST", "localhost")
COUCHDB_PORT = os.getenv("COUCHDB_PORT", "5984")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

# This value is passed to KafkaETL and determines how long a consumer waits for messages
# before its processing loop (e.g., 'for message in consumer:') finishes.
KAFKA_CONSUMER_TIMEOUT_MS = int(
    os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "1200000")
)  # Default 20 minutes

# Delay before restarting a thread that has finished its work cycle (e.g., due to consumer timeout)
THREAD_RESTART_DELAY_S = int(
    os.getenv("THREAD_RESTART_DELAY_S", "10")
)  # Default 10 seconds

COUCHDB_URL = f"http://{COUCHDB_UNAME}:{COUCHDB_PWD}@{COUCHDB_HOST}:{COUCHDB_PORT}/"

# Event to signal all threads to shut down gracefully
shutdown_event = threading.Event()


def kafka_loader_worker(kettle_instance, load_method_name, consumer_name):
    """
    Worker function to be run in a thread.
    It calls the specified load method on the kettle_instance.
    The thread will naturally exit when the load_method's consumer times out.
    """
    thread_name = threading.current_thread().name
    log.info(f"[{thread_name}] for '{consumer_name}': Starting worker...")
    try:
        # Get the actual load method from the kettle instance
        method_to_call = getattr(kettle_instance, load_method_name)
        # The load_X method in KafkaETL will loop until its consumer times out
        method_to_call()

        # This part is reached if the consumer timed out and the load_method completed its current run
        if not shutdown_event.is_set():
            log.info(
                f"[{thread_name}] for '{consumer_name}': Processing cycle finished (consumer timeout). Will be restarted by main thread."
            )
    except Exception as e:
        if not shutdown_event.is_set():  # Avoid error messages if we are shutting down
            log.error(
                f"[{thread_name}] for '{consumer_name}': Error during processing: {e}"
            )
            log.error(traceback.format_exc())
            # You might want to add more specific error handling or backoff logic here
    finally:
        if shutdown_event.is_set():
            log.info(
                f"[{thread_name}] for '{consumer_name}': Shutdown signal received, exiting."
            )
        else:
            log.info(f"[{thread_name}] for '{consumer_name}': Worker exiting.")


def main():
    log.info(
        f"Attempting to initialize KafkaETL with CouchDB: {COUCHDB_HOST}:{COUCHDB_PORT}, Kafka: {KAFKA_BOOTSTRAP_SERVERS}"
    )
    log.info(f"Kafka consumer timeout set to: {KAFKA_CONSUMER_TIMEOUT_MS} ms")
    log.info(f"Thread restart delay set to: {THREAD_RESTART_DELAY_S} seconds")

    try:
        kettle = KafkaETL(
            couchdb_url=COUCHDB_URL,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            ctimeout_ms=KAFKA_CONSUMER_TIMEOUT_MS,  # Pass the timeout to the ETL class
        )
        log.info("Kafka ETL object is instantiated and ready to listen...")
    except Exception as e:
        log.error(f"FATAL: Could not initialize KafkaETL: {e}")
        return  # Exit if kettle can't be initialized

    # Define the tasks: (Thread Name, Kettle Method Name, Friendly Consumer Name for logs)
    tasks_to_run = [
        ("FreeWeatherThread", "load_free_weather", "free-weather"),
        ("OpenWeatherThread", "load_open_weather", "open-weather"),
        ("OpenMeteoThread", "load_open_meteo", "open-meteo"),
    ]

    active_threads = {}

    try:
        # Main loop to manage and restart threads
        while not shutdown_event.is_set():
            for thread_name, method_name, consumer_log_name in tasks_to_run:
                if (
                    thread_name not in active_threads
                    or not active_threads[thread_name].is_alive()
                ):
                    if (
                        thread_name in active_threads
                    ):  # Means the thread died or finished
                        log.info(
                            f"[MainLoop] Thread '{thread_name}' for '{consumer_log_name}' is not alive. Restarting after {THREAD_RESTART_DELAY_S}s delay."
                        )
                        time.sleep(THREAD_RESTART_DELAY_S)  # Wait before restarting
                        if shutdown_event.is_set():
                            break  # Check again if shutdown was called during sleep

                    log.info(
                        f"[MainLoop] Starting thread '{thread_name}' for '{consumer_log_name}'..."
                    )
                    thread = threading.Thread(
                        target=kafka_loader_worker,
                        args=(kettle, method_name, consumer_log_name),
                        name=thread_name,
                        daemon=True,  # Daemon threads exit when the main program exits
                    )
                    active_threads[thread_name] = thread
                    thread.start()

            if shutdown_event.is_set():
                break
            time.sleep(5)  # Check thread status every 5 seconds

    except KeyboardInterrupt:
        log.error(
            "\n[MainLoop] KeyboardInterrupt received. Signaling threads to shut down..."
        )
    except Exception as e:
        log.error(
            f"[MainLoop] An unexpected error occurred: {e}. Signaling shutdown..."
        )
    finally:
        shutdown_event.set()  # Ensure shutdown_event is set in all exit paths
        log.info("[MainLoop] Waiting for threads to complete shutdown...")

        all_threads = list(active_threads.values())  # Get a list of thread objects
        for thread in all_threads:
            if thread.is_alive():
                log.info(f"[MainLoop] Waiting for {thread.name} to join...")
                # The consumer_timeout_ms in KafkaETL means threads should exit on their own
                # after that period if no new messages. Join with a timeout slightly longer.
                thread.join(timeout=(KAFKA_CONSUMER_TIMEOUT_MS / 1000) + 5)
                if thread.is_alive():
                    log.info(f"[MainLoop] Thread {thread.name} did not join in time.")

        log.info("[MainLoop] All threads processed for shutdown. Exiting application.")


with DAG(
    dag_id="kafka_to_couchdb_loader",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="* * * * *",
    catchup=False,
    tags=["weather", "kafka", "load-to-couchdb"],
    doc_md="Loader from Kafka to CouchDB",
) as dag:
    fetch_open_weather_task = PythonOperator(
        task_id="fetch_and_send_open_weather",
        python_callable=main,
    )
