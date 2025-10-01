import polars as pl
from prophet import Prophet
import pickle
from pathlib import Path


class CurrencyPredictor:
    def __init__(self):
        self.model = None
        self.currency = None

    def train(self, df: pl.DataFrame, currency: str):
        """Treina modelo Prophet para previsão"""
        self.currency = currency

        # Preparar dados para Prophet (precisa de colunas 'ds' e 'y')
        df_prophet = (
            df.select(
                [
                    pl.col("time_last_update_utc").alias("ds"),
                    pl.col("exchange_rate").alias("y"),
                ]
            )
            .sort("ds")
            .to_pandas()  # Prophet usa Pandas
        )

        # Treinar modelo
        self.model = Prophet(
            daily_seasonality=False,
            weekly_seasonality=True,
            yearly_seasonality=False,
            changepoint_prior_scale=0.05,
        )
        self.model.fit(df_prophet)

    def predict(self, periods: int = 7) -> pl.DataFrame:
        """Faz previsão para os próximos N dias"""
        if self.model is None:
            raise ValueError("Modelo não treinado. Execute .train() primeiro.")

        # Criar dataframe futuro
        future = self.model.make_future_dataframe(periods=periods)
        forecast = self.model.predict(future)

        # Converter para Polars e pegar apenas previsões futuras
        df_forecast = pl.from_pandas(
            forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
        )
        return df_forecast.tail(periods)

    def save(self, path: str = "models/currency_predictor.pkl"):
        """Salva modelo treinado"""
        Path(path).parent.mkdir(exist_ok=True)
        with open(path, "wb") as f:
            pickle.dump((self.model, self.currency), f)

    def load(self, path: str = "models/currency_predictor.pkl"):
        """Carrega modelo salvo"""
        with open(path, "rb") as f:
            self.model, self.currency = pickle.load(f)
