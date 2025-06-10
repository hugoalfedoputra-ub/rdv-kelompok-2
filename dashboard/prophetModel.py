import pandas as pd
from prophet import Prophet

class ProphetWrapper:
    def __init__(self, **kwargs):
        """
        Wrapper untuk Prophet.
        kwargs diteruskan ke Prophet(), misalnya: daily_seasonality=True, seasonality_mode='multiplicative', dll.
        """
        self.model = Prophet(**kwargs)
        self.fitted = False
        self.forecast = None

    def fit(self, df: pd.DataFrame):
        """
        Melatih model Prophet.
        df harus memiliki kolom 'ds' (datetime) dan 'y' (nilai target).
        """
        if not {'ds', 'y'}.issubset(df.columns):
            raise ValueError("DataFrame harus memiliki kolom 'ds' dan 'y'")
        self.model.fit(df)
        self.df_train = df.copy()
        self.fitted = True

    def predict(self, periods: int, freq: str = 'H', only_future: bool = True) -> pd.DataFrame:
        """
        Membuat prediksi ke depan sebanyak 'periods' langkah dengan frekuensi 'freq'.
        only_future:
            - True: hanya return prediksi masa depan
            - False: return semua prediksi (masa lalu + masa depan)
        Return:
            - pd.Series berisi nilai yhat hasil prediksi
        """
        if not self.fitted:
            raise ValueError("Model belum dilatih. Jalankan .fit(df) terlebih dahulu.")
        future = self.model.make_future_dataframe(periods=periods, freq=freq)
        self.forecast = self.model.predict(future)

        if only_future:
            forecast_tail = self.forecast.tail(periods)
            return forecast_tail[['ds', 'yhat']]
        else:
            return self.forecast[['ds', 'yhat']]

    def plot(self):
        """Plot hasil prediksi jika sudah ada."""
        if self.forecast is not None:
            return self.model.plot(self.forecast)
        else:
            raise ValueError("Belum ada hasil prediksi. Jalankan .predict() terlebih dahulu.")

    def plot_components(self):
        """Plot komponen seperti trend dan seasonality."""
        if self.forecast is not None:
            return self.model.plot_components(self.forecast)
        else:
            raise ValueError("Belum ada hasil prediksi. Jalankan .predict() terlebih dahulu.")
