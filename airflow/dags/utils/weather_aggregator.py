from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col

class Aggregator:
    def __init__(self, column_map:dict=None):
        if column_map==None:
            self.column_map={
                # Time
                "localtime": "time",

                # Temperature
                "temperature_2m": "temperature_c",
                "temp": "temperature_c",
                "temperature_c": "temperature_c",

                # Feels like
                "apparent_temperature": "feels_like_c",
                "feelslike_c": "feels_like_c",
                "feels_like": "feels_like_c",

                # Humidity
                "relative_humidity_2m": "humidity_pct",
                "humidity": "humidity_pct",

                # Wind speed
                "wind_speed_10m": "wind_speed_kmph",
                "wind_kph": "wind_speed_kmph",
                "wind_speed": "wind_speed_kmph",

                # Wind gust
                "wind_gusts_10m": "wind_gust_kmph",
                "gust_kph": "wind_gust_kmph",
                "wind_gust": "wind_gust_kmph",

                # Wind degree
                "wind_direction_10m": "wind_degree",

                # Pressure
                "pressure_mb" : "pressure",
                "pressure_msl" : "pressure",

                # Cloud
                "cloud_cover": "cloud_total_pct",
                "cloud": "cloud_total_pct",
                "all": "cloud_total_pct",

                # Lokasi
                "district": "district",
                "latitude": "latitude",
                "lat" : "latitude",
                "longitude": "longitude",
                "lon" : "longitude"
            }
        else :
            self.column_map = column_map

    def standardize_columns(self, df:DataFrame, col_map:dict):
        """
        Fungsi untuk menyamakan nama-nama kolom dari sumber data yang berbeda
        """
        if col_map == None:
            col_map = self.column_map
        
        renamed_cols = [col(c).alias(col_map[c]) for c in df.columns if c in col_map]
        return df.select(*renamed_cols)
    
    def aggregate_common_columns(dfs: list[DataFrame], group_cols: list[str] = ["district", "latitude", "longitude"]) -> DataFrame:
        """
        Menggabungkan dan mengagregasi kolom-kolom yang sama dari beberapa DataFrame PySpark.
        
        Args:
            dfs (List[DataFrame]): Daftar DataFrame PySpark.
            group_cols (List[str]): Kolom yang dijadikan dasar pengelompokan/agregasi (group by berdasarkan kolom apa) misal : ["district", "latitude", "longitude"].
        
        Returns:
            DataFrame: DataFrame hasil agregasi rata-rata kolom umum.
        """
        if len(dfs) < 2:
            raise ValueError("Minimal dua DataFrame diperlukan.")

        # 1. Ambil kolom yang sama di semua DataFrame
        common_cols = set(dfs[0].columns)
        for df in dfs[1:]:
            common_cols &= set(df.columns)
        common_cols = list(common_cols)

        if not all(col in common_cols for col in group_cols):
            raise ValueError(f"Semua group_cols ({group_cols}) harus ada di kolom umum.")

        # 2. Select hanya kolom yang sama
        dfs_selected = [df.select(common_cols) for df in dfs]

        # 3. Union semua DataFrame
        union_df = dfs_selected[0]
        for df in dfs_selected[1:]:
            union_df = union_df.unionByName(df)

        # 4. Agregasi
        value_cols = [col for col in common_cols if col not in group_cols]
        agg_exprs = [avg(col).alias(f"avg_{col}") for col in value_cols]
        result_df = union_df.groupBy(*group_cols).agg(*agg_exprs)

        return result_df