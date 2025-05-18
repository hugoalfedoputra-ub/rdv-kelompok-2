import json
import hashlib
import datetime
import pycouchdb
from kafka import KafkaConsumer

class WeatherModel:
    def __init__(self, url, db_name, username=None, password=None):
        self.url = url
        self.db_name = db_name
        self.auth = (username, password) if username and password else None
        self.server = pycouchdb.Server(self.url, auth=self.auth)
        try:
            self.db = self.server.database(db_name)
        except pycouchdb.exceptions.NotFound:
            self.db = self.server.create(db_name)

    def _generate_id(self, district, time):
        hash_input = (str(district) + str(time)).encode('utf-8')
        return hashlib.sha256(hash_input).hexdigest()

    def _enrich_row_with_metadata(self, row):
        row_dict = row.asDict()
        row_dict['_id'] = self._generate_id(row_dict.get('district'), row_dict.get('time'))
        row_dict['last_updated'] = datetime.datetime.now().isoformat()
        return row_dict

    def _post_batch(self, batch, count):
        try:
            self.db.save_bulk(batch)
            print(f"Batch {count} berhasil disimpan ke CouchDB.")
        except Exception as e:
            raise Exception(f"Gagal menyimpan batch {count}: {str(e)}")
        
    def save_to_couchdb_batch(self, spark_df, batch_size=1000):
        records_rdd = spark_df.rdd.map(self._enrich_row_with_metadata)
        batch = []
        count = 0

        for record in records_rdd.toLocalIterator():
            batch.append(record)
            if len(batch) == batch_size:
                self._post_batch(batch, count)
                batch = []
                count += 1

        if batch:
            self._post_batch(batch, count)

        print("Semua data berhasil disimpan ke CouchDB.")

    def save_weather_from_kafka(self, topic, bootstrap_servers, group_id='weather-group'):
        """
        Fungsi untuk mendengarkan Kafka dan menyimpan data ke CouchDB.
        """
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=False
        )

        for message in consumer:
            try:
                data = message.value
                location = data['data']['location']
                last_updated = data['data']['current']['last_updated']
                _id = self._generate_id(location, last_updated)

                entry = {
                    '_id': _id,
                    'provider': data['provider'],
                    'location': location,
                    'current_weather': data['data']['current'],
                    'created_at': datetime.datetime.now().isoformat(),
                    'last_updated': last_updated
                }

                self.db.save(entry)
                print(f"Data disimpan: {_id}")
                consumer.commit()
            except Exception as e:
                print(f"Error: {e}")
                continue
