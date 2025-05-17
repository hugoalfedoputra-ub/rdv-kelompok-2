import os
import sys
import time
import hashlib
import requests

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, sum
from pyspark.sql import DataFrame

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

class SparkWeatherLoader():
    def __init__(self):
        self.spark = self.getSparkSession(appName="SparkWeatherLoader")

    def getSparkSession(self, appName:str)->SparkSession:
        spark = SparkSession.builder \
            .appName(appName) \
            .master("local[*]") \
            .getOrCreate()
        return spark
    
    def fetchHistorical(self, features:list, locations:dict[str, tuple], start_date:str, end_date:str) -> DataFrame:
        final_psdf = None

        for district in locations.keys() :
            print(f"Start fetch data for {district}")
            latitude = locations[district][0]
            longitude = locations[district][1]
            for year in range(2017, 2025+1):
                start_date = f"{year}-01-01"
                temp_end_date = f"{year}-12-31"
                
                if year == 2025:
                    temp_end_date = end_date
                    
                url = f"https://archive-api.open-meteo.com/v1/archive?latitude={latitude}&longitude={longitude}&start_date={start_date}&end_date={temp_end_date}&hourly={features}&timezone=auto"
                
                success = False
                attempts = 0

                while not success and attempts < 3:
                    try:
                        print(f"Getting data for {year} from {start_date} to {temp_end_date}")
                        res = requests.get(url)

                        if res.status_code == 200:
                            data = res.json()['hourly']
                            num_records = len(data['time'])
                            
                            rows = [
                                {key: data[key][i] for key in data}
                                for i in range(num_records)
                            ]
                            
                            psdf = self.spark.read.json(self.spark.sparkContext.parallelize(rows))
                            psdf = psdf.withColumn("district", lit(district))\
                                    .withColumn("latitude", lit(latitude))\
                                    .withColumn("longitude", lit(longitude))

                            if final_psdf is None:
                                final_psdf = psdf
                            else:
                                final_psdf = final_psdf.unionByName(psdf)

                            if "_corrupt_record" in psdf.columns:
                                psdf = psdf.drop("_corrupt_record")
                            
                            success = True
                            print(f"Successfully fetched {num_records} records for {year}.")
                            time.sleep(60)

                        elif res.status_code == 429:
                            print(f"Rate limit reached, retrying... (Attempt {attempts + 1}/3)")
                            time.sleep(60)
                            attempts += 1
                        elif res.status_code == 443:
                            print(f"Error status code 443, retrying... (Attempt {attempts + 1}/3)")
                            time.sleep(60)
                            attempts += 1
                        else:
                            print(f"Failed to get data for {year}. Status code: {res.status_code}")
                            break
                    except Exception as e:
                        print(f"Error while getting data for {year}. Message: {e}. Retrying...")
                        time.sleep(60)
                        attempts += 1

                if not success:
                    print(f"Failed to retrieve data for {year} after {attempts} attempts.")

            if final_psdf is not None:
                print(f"Total records fetched: {final_psdf.count()}")
            else:
                print("No data was successfully retrieved.")

            print(f"Done fetch data for {district}")
            
            return final_psdf
        
    def getMissingEachColumn(psdf:DataFrame):
        missing_counts = psdf.select([
            sum(col(c).isNull().cast("int")).alias(c) for c in psdf.columns
        ])
        return missing_counts
    
    def fetchRecent(self, features:list, locations:dict[str, tuple], day_length:int) -> DataFrame:
        final_psdf = None
        today = datetime.today()
        end_date = today.strftime("%Y-%m-%d")
        start_date = today - timedelta(days=day_length)

        success = False
        attempts = 0

        while not success and attempts < 3:
            for district in locations:
                latitude = locations[district][0]
                longitude = locations[district][1]
                
                forecast_url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&hourly={features}&start_date={start_date}&end_date={end_date}"

                try:
                    print(f"Getting data for {district} from {start_date} to {end_date}")
                    response = requests.get(forecast_url)
                    if response.status_code == 200:
                        missing_data = response.json()['hourly']
                        rows_oriented_missing = [
                            {key: missing_data[key][i] for key in missing_data}
                            for i in range(len(missing_data['time']))
                        ]
                        recent_psdf = self.spark.read.json(self.spark.sparkContext.parallelize(rows_oriented_missing))
                        recent_psdf = recent_psdf.withColumn("district", lit(district)) \
                                .withColumn("longitude", lit(longitude)) \
                                .withColumn("latitude", lit(latitude))
                        if final_psdf is None:
                            final_psdf = recent_psdf
                        else :
                            final_psdf = final_psdf.unionByName(recent_psdf)

                        success = True
                    else :
                        print(f"Can't fetch data, status code: {response.status_code}")
                        print(f"Retrying... (Attempts {attempts+1}/3)")
                        attempts += 1
                except Exception as e:
                    print(f"Error while getting recent data for {district}. Message: {e}. Retrying...")
                    print(f"Retrying... (Attempts {attempts+1}/3)")
                    attempts += 1

        return final_psdf
