import datetime
import pycouchdb
import pandas as pd
import requests
import os, shutil
import re
from dotenv import load_dotenv

load_dotenv()

from r2client.R2Client import R2Client as r2

ACCOUNT_ID = os.getenv("ACCOUNT_ID")
AWS_SECRET_KEY_ID = os.getenv("AWS_SECRET_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# This will prepend rdv/ to all data for this project
S3_RDV_ROOT = "rdv/"

# This will make a folder called temp/ in src/bulk_etl/
BULK_CSV_TEMP_LOC = "temp_csv/"

CSV_EXT = ".csv"


class BulkETL:
    def __init__(self, couchdb_url):
        self.open_meteo_url = ""
        self.start_date = ""
        self.end_date = ""
        self.latitude = ""
        self.longitude = ""
        self.data = None
        self.server_exists = False
        self.server = None
        self.s3 = None
        self.s3_bucket_name = None

        self.connect_to_couchdb(couchdb_url=couchdb_url)

        self.connect_to_s3()

    def configure_url(self, start_date, end_date, latitude, longitude):
        self.start_date = start_date
        self.end_date = end_date
        self.latitude = latitude
        self.longitude = longitude
        self.open_meteo_url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,is_day,relative_humidity_2m,dew_point_2m,apparent_temperature,precipitation,weather_code,pressure_msl,surface_pressure,cloud_cover,cloud_cover_low,cloud_cover_mid,cloud_cover_high,et0_fao_evapotranspiration,vapour_pressure_deficit,wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=auto"

    def connect_to_couchdb(self, couchdb_url):
        self.server = pycouchdb.Server(couchdb_url, authmethod="basic")
        try:
            self.server.info()
            print(f"Successfully connected to CouchDB at {couchdb_url.split('@')[-1]}")
            self.server_exists = True
        except Exception as e:
            print(f"Failed to connect to CouchDB: {e}")
            self.server_exists = False
            raise

    def connect_to_s3(self):
        if self.server_exists:
            self.s3 = r2(
                access_key=AWS_SECRET_KEY_ID,
                secret_key=AWS_SECRET_ACCESS_KEY,
                endpoint=f"https://{ACCOUNT_ID}.r2.cloudflarestorage.com",
            )
            print("S3 client for Cloudflare R2 successfully created")
            self.s3_bucket_name = "work"
        else:
            print(
                "CouchDB server is not running, creation of S3 client for Cloudflare R2 is aborted. Please successfully connect to CouchDB first."
            )

    def _get_open_meteo(self):
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

    def get_bulk_then_upload(self):
        if int(self.end_date.split("-")[0]) - int(self.start_date.split("-")[0]) > 1:
            print(
                "WARNING: YOU ARE GOING TO REQUEST MORE THAN ONE YEAR'S WORTH OF DATA FROM THE API.\n\tTHIS WILL COST YOU A LOT OF API CALLS (UPWARDS OF HUNDREDS).\n\tPLEASE BE SURE YOU ARE NOT RUNNING THIS MULTIPLE TIMES AS IT WILL DEPLETE YOUR DAILY LIMIT!"
            )

        print("Fetching data, this might take some time...")
        self.data = self._get_open_meteo()

        if self.data == None:
            print("No data. Aborting process...")
            return

        print("Processing data...")
        # Split the historical data to descriptor (the metadata) and final (the actual data)
        descriptor = pd.DataFrame(
            {
                "latitude": [self.data["latitude"]],
                "longitude": [self.data["longitude"]],
                "utc_offset_seconds": [self.data["utc_offset_seconds"]],
                "timezone": [self.data["timezone"]],
                "timezone_abbreviation": [self.data["timezone_abbreviation"]],
            }
        )

        final = pd.DataFrame(
            {
                "datetime": self.data["hourly"]["time"],
                "temperature_2m": self.data["hourly"]["temperature_2m"],
                "is_day": self.data["hourly"]["is_day"],
                "relative_humidity_2m": self.data["hourly"]["relative_humidity_2m"],
                "dew_point_2m": self.data["hourly"]["dew_point_2m"],
                "apparent_temperature": self.data["hourly"]["apparent_temperature"],
                "precipitation": self.data["hourly"]["precipitation"],
                "weather_code": self.data["hourly"]["weather_code"],
                "pressure_msl": self.data["hourly"]["pressure_msl"],
                "surface_pressure": self.data["hourly"]["surface_pressure"],
                "cloud_cover": self.data["hourly"]["cloud_cover"],
                "cloud_cover_low": self.data["hourly"]["cloud_cover_low"],
                "cloud_cover_mid": self.data["hourly"]["cloud_cover_mid"],
                "cloud_cover_high": self.data["hourly"]["cloud_cover_high"],
                "et0_fao_evapotranspiration": self.data["hourly"][
                    "et0_fao_evapotranspiration"
                ],
                "vapour_pressure_deficit": self.data["hourly"][
                    "vapour_pressure_deficit"
                ],
                "wind_speed_10m": self.data["hourly"]["wind_speed_10m"],
                "wind_direction_10m": self.data["hourly"]["wind_direction_10m"],
                "wind_gusts_10m": self.data["hourly"]["wind_gusts_10m"],
            }
        )

        # Create a timestamp string for file naming
        ct = datetime.datetime.now()
        strct = re.sub(r"[ .:]", "_", str(ct))

        # Create the temp folder to store csvs to be uploaded to S3
        try:
            os.makedirs(BULK_CSV_TEMP_LOC, exist_ok=True)
        except Exception as e:
            print(e)

        descriptor_file_name = (
            str(self.data["latitude"]).replace(".", "_")
            + "_"
            + str(self.data["longitude"]).replace(".", "_")
            + "_descriptor_"
            + strct
            + CSV_EXT
        )
        final_file_name = (
            str(self.data["latitude"]).replace(".", "_")
            + "_"
            + str(self.data["longitude"]).replace(".", "_")
            + "_data_"
            + strct
            + CSV_EXT
        )

        # Save from DataFrame to csv
        print("Uploading data...")
        try:
            descriptor.to_csv(BULK_CSV_TEMP_LOC + descriptor_file_name, index=False)
            final.to_csv(BULK_CSV_TEMP_LOC + final_file_name, index=False)
        except Exception as e:
            print(e)
            raise

        # Saves metadata to CouchDB so that end user can query database to get the
        # file name instead of listing all object from S3
        db_s3_md = self.server.database("s3_metadata")

        entry = {
            "descriptor_file_name": descriptor_file_name,
            "data_file_name": final_file_name,
        }

        # Save the metadata entry first before to S3, just in case the CouchDB connection
        # has issues at this point.
        # Basically, we don't want a successful S3 upload without the metadata being stored
        # properly in the first place.
        try:
            db_s3_md.save(entry)
        except Exception as e:
            print(e)
            print("Metadata save to CouchDB failed. Aborting object upload to S3...")
            return

        # Upload the files to S3
        try:
            self.s3.upload_file(
                self.s3_bucket_name,
                BULK_CSV_TEMP_LOC + descriptor_file_name,
                S3_RDV_ROOT + descriptor_file_name,
            )
            self.s3.upload_file(
                self.s3_bucket_name,
                BULK_CSV_TEMP_LOC + final_file_name,
                S3_RDV_ROOT + final_file_name,
            )
        except Exception as e:
            print(e)
            raise

        # Delete the temp csv files
        for filename in os.listdir(BULK_CSV_TEMP_LOC):
            file_path = os.path.join(BULK_CSV_TEMP_LOC, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
            except Exception as e:
                print("Failed to delete %s. Reason: %s" % (file_path, e))

        return descriptor_file_name, final_file_name

    def run(self, start_date, end_date, latitude, longitude):
        """
        If end user wants a one function to conveniently call
        """
        self.configure_url(start_date, end_date, latitude, longitude)

        # Assuming CouchDB connection and S3 client creation is successful
        dfn, ffn = self.get_bulk_then_upload()
        print(dfn)
        print(ffn)
