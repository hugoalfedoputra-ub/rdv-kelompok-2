import json
import hashlib
import datetime
import requests

class CouchDBClient():
    def __init__(self, url, db_name, username, password):
        self.db_url = f"{url}/{db_name}"
        self.auth = (username, password) if username and password else None
        self.headers = {"Content-Type": "application/json"}
        self._ensure_db_exists()

    def _ensure_db_exists(self):
        res = requests.put(self.db_url, auth=self.auth)
        if res.status_code not in [201, 412]:
            raise Exception(f"Gagal membuat database: {res.text}")

    def _generate_id(self, district, time):
        """
        Fungsi ini bertujuan untuk melakukan hash dari kolom district dan time
        Sedangkan penambahan hash digunakan untuk membuat id dari tiap record,--
        untuk memudahkan proses CRUD
        """
        hash_input = (str(district) + str(time)).encode('utf-8')
        return hashlib.sha256(hash_input).hexdigest()

    def _enrich_row_with_metadata(self, row):
        """
        Fungsi ini bertujuan untuk menambahkan metadata ke dalam dataset yang dataset sebelum ke databse
        Metadata yang ditambahkan:
            - "_id" : id berupa hash dari informasi kolom
            - "last_updated" : waktu terakhir data di update
        """
        row_dict = row.asDict()
        row_dict['_id'] = self._generate_id(row_dict.get('district'), row_dict.get('time'))
        row_dict['last_updated'] = datetime.datetime.now().isoformat()
        return row_dict


    def _post_batch_to_couchdb(batch, bulk_url, headers, auth, count):
        payload = {"docs": batch}
        response = requests.post(bulk_url, data=json.dumps(payload), headers=headers, auth=auth)
        if response.status_code != 201:
            raise Exception(f"Gagal menyimpan batch {count}: {response.text}")
        print(f"Batch {count} berhasil disimpan ke CouchDB.")

    def save_to_couchdb_batch(self, spark_df, couchdb_url, db_name, username=None, password=None, batch_size=1000):
        auth = (username, password) if username and password else None
        db_url = f"{couchdb_url}/{db_name}"

        bulk_url = f"{db_url}/_bulk_docs"
        headers = {"Content-Type": "application/json"}

        records_rdd = spark_df.rdd.map(self._enrich_row_with_metadata)

        batch = []
        count = 0
        for record in records_rdd.toLocalIterator():
            batch.append(record)
            if len(batch) == batch_size:
                self._post_batch_to_couchdb(batch, bulk_url, headers, auth, count)
                batch = []
                count += 1

        if batch:
            self._post_batch_to_couchdb(batch, bulk_url, headers, auth, count)

        print("Semua data berhasil disimpan ke CouchDB.")



