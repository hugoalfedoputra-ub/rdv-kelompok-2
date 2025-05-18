import json
from kafka import KafkaConsumer
import datetime
import pycouchdb
import hashlib
import os


class KafkaETL:
    def __init__(
        self,
        couchdb_url,  # e.g., "http://user:pass@couchdb-host:5984/"
        bootstrap_servers=["localhost:29092"],  # e.g., ["kafka-host:9092"]
        ctimeout_ms=1200000,  # 20 minutes
    ):
        # The couchdb_url should now contain username, password, host, and port
        self.server = pycouchdb.Server(couchdb_url, authmethod="basic")
        try:
            self.server.info()  # Test connection
            print(f"Successfully connected to CouchDB at {couchdb_url.split('@')[-1]}")
        except Exception as e:
            print(f"Failed to connect to CouchDB: {e}")
            raise  # Re-raise the exception to stop initialization if connection fails

        self.bootstrap_servers = bootstrap_servers
        self.consumer_timeout_ms = ctimeout_ms

        # Ensure bootstrap_servers is a list of strings
        if isinstance(self.bootstrap_servers, str):
            self.bootstrap_servers = [
                s.strip() for s in self.bootstrap_servers.split(",")
            ]

        print(f"Attempting to connect to Kafka brokers: {self.bootstrap_servers}")

        try:
            self.fw_consumer = KafkaConsumer(
                "free-weather",
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                client_id="free-weather-consumer",
                value_deserializer=lambda m: json.loads(m.decode("ascii")),
                consumer_timeout_ms=ctimeout_ms,
            )
            print("Successfully created KafkaConsumer for 'free-weather'")

            self.ow_consumer = KafkaConsumer(
                "open-weather",
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                client_id="open-weather-consumer",
                value_deserializer=lambda m: json.loads(m.decode("ascii")),
                consumer_timeout_ms=ctimeout_ms,
            )
            print("Successfully created KafkaConsumer for 'open-weather'")

            self.om_consumer = KafkaConsumer(
                "open-meteo",
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                client_id="open-meteo-consumer",
                value_deserializer=lambda m: json.loads(m.decode("ascii")),
                consumer_timeout_ms=ctimeout_ms,
            )
            print("Successfully created KafkaConsumer for 'open-meteo'")

        except Exception as e:
            print(f"Failed to create Kafka consumer: {e}")
            raise  # Re-raise

    def load_free_weather(self):
        db_fw = self.server.database("free_weather")

        for message in self.fw_consumer:
            data = message.value
            print(data["provider"])

            m = hashlib.sha256()
            last_updated = str(message.timestamp)
            res = "".join(format(ord(i), "08b") for i in last_updated).encode("utf-8")
            m.update(res)
            id = m.hexdigest()

            ct = datetime.datetime.now()

            entry = {
                "_id": id,
                "provider": data["provider"],
                "location": data["data"]["location"],
                "current_weather": data["data"]["current"],
                "created_at": str(ct),
                "last_updated": data["data"]["current"]["last_updated"],
            }

            try:
                res = db_fw.save(entry)
                self.fw_consumer.commit()
            except Exception as e:
                print(e)
                continue

    def load_open_weather(self):
        db_ow = self.server.database("open_weather")

        for message in self.ow_consumer:
            data = message.value
            print(data["provider"])

            ct = datetime.datetime.now()

            m = hashlib.sha256()
            last_updated = str(datetime.datetime.fromtimestamp(data["data"]["dt"]))
            res = "".join(format(ord(i), "08b") for i in last_updated).encode("utf-8")
            m.update(res)
            id = m.hexdigest()

            entry = {
                "_id": id,
                "provider": data["provider"],
                "location": {
                    "lat": data["data"]["coord"]["lat"],
                    "lon": data["data"]["coord"]["lon"],
                    "name": data["data"]["name"],
                    "country": data["data"]["sys"]["country"],
                },
                "current_weather": {
                    "clouds": data["data"]["clouds"],
                    "main": {
                        "feels_like": data["data"]["main"]["feels_like"],
                        "pressure_ground_level": data["data"]["main"]["grnd_level"],
                        "humidity": data["data"]["main"]["humidity"],
                        "pressure": data["data"]["main"]["pressure"],
                        "sea_level": data["data"]["main"]["sea_level"],
                        "temp": data["data"]["main"]["temp"],
                        "visibility": data["data"]["visibility"],
                        "wind_dir": data["data"]["wind"]["deg"],
                        "wind_speed": data["data"]["wind"]["speed"],
                        "wind_gust": data["data"]["wind"]["gust"],
                    },
                    "weather": data["data"]["weather"],
                },
                "created_at": str(ct),
                "last_updated": str(
                    datetime.datetime.fromtimestamp(data["data"]["dt"])
                ),
            }

            try:
                res = db_ow.save(entry)
                self.ow_consumer.commit()
            except Exception as e:
                print(e)
                continue

    def load_open_meteo(self):
        db_om = self.server.database("open_meteo")

        for message in self.om_consumer:
            data = message.value
            print(data["provider"])

            m = hashlib.sha256()
            last_updated = str(message.timestamp)
            res = "".join(format(ord(i), "08b") for i in last_updated).encode("utf-8")
            m.update(res)
            id = m.hexdigest()

            ct = datetime.datetime.now()

            entry = {
                "_id": id,
                "provider": data["provider"],
                "location": {
                    "lat": data["latitude"],
                    "lon": data["longitude"],
                    "grid_lat": data["data"]["latitude"],
                    "grid_lon": data["data"]["longitude"],
                },
                "current_weather": data["data"]["current"],
                "created_at": str(ct),
                "last_updated": data["data"]["current"]["time"],
            }

            try:
                res = db_om.save(entry)
                self.om_consumer.commit()
            except Exception as e:
                print(e)
                continue
