import polars as pl
from pathlib import Path


class DataLoader:
    def __init__(self, data_path: str = "data/gold/finance"):
        self.data_path = Path(data_path)

    def load_heatmap(self) -> pl.DataFrame:
        return pl.read_parquet(self.data_path / "currency_heatmap.parquet")

    def load_top_gainers(self) -> pl.DataFrame:
        return pl.read_parquet(self.data_path / "top_gainers.parquet")

    def load_top_losers(self) -> pl.DataFrame:
        return pl.read_parquet(self.data_path / "top_losers.parquet")

    def load_volatility(self) -> pl.DataFrame:
        return pl.read_parquet(self.data_path / "volatility_exchange_rates.parquet")

    def load_historical(self, currency: str) -> pl.DataFrame:
        """Carrega histórico de uma moeda específica"""
        # Assumindo que você tem um parquet com histórico completo
        df = pl.read_parquet("data/silver/finance/exchange_rates/*.parquet")
        return df.filter(pl.col("code_exchange_rate") == currency)
