from src.utils import PolarsProcessor
from time import time
import polars as pl
from rich import print


class Gold(PolarsProcessor):
    def __init__(self):
        super().__init__()

    def execute(self):
        source_path = "data/silver/finance/exchange_rates"
        lazy_df = self.polars_read_parquet(source_path)

        # Calcular variação diária com lag
        df_performance = (lazy_df
            .sort(["code_exchange_rate", "time_last_update_utc"])
            .with_columns([
                pl.col("exchange_rate").shift(1).over("code_exchange_rate").alias("prev_rate")
            ])
            .with_columns([
                (
                    (pl.col("exchange_rate") - pl.col("prev_rate")) / pl.col("prev_rate") * 100
                ).round(2).alias("daily_change_pct")
            ])
            .filter(pl.col("prev_rate").is_not_null())  # Remove primeira linha de cada moeda
        )

        # Calcular variações semanais e mensais
        df_performance = (df_performance
            .with_columns([
                pl.col("exchange_rate").shift(7).over("code_exchange_rate").alias("rate_7d_ago"),
                pl.col("exchange_rate").shift(30).over("code_exchange_rate").alias("rate_30d_ago")
            ])
            .with_columns([
                (
                    (pl.col("exchange_rate") - pl.col("rate_7d_ago")) / pl.col("rate_7d_ago") * 100
                ).round(2).alias("weekly_change_pct"),
                (
                    (pl.col("exchange_rate") - pl.col("rate_30d_ago")) / pl.col("rate_30d_ago") * 100
                ).round(2).alias("monthly_change_pct")
            ])
        )

        # Pegar última data disponível
        last_date = df_performance.select(pl.col("time_last_update_utc").max()).collect().item()

        df_movers = df_performance.filter(pl.col("time_last_update_utc") == last_date)

        # Top 10 Gainers
        top_gainers = (df_movers
            .sort("daily_change_pct", descending=True)
            .head(10)
            .with_row_index("rank")
            .with_columns((pl.col("rank") + 1).alias("rank"))
            .select(["rank", "code_exchange_rate", "exchange_rate", "daily_change_pct"])
        )

        # Top 10 Losers
        top_losers = (df_movers
            .sort("daily_change_pct", descending=False)
            .head(10)
            .with_row_index("rank")
            .with_columns((pl.col("rank") + 1).alias("rank"))
            .select(["rank", "code_exchange_rate", "exchange_rate", "daily_change_pct"])
        )


        # Tabela para dashboard de heat map
        df_heatmap = (df_movers
            .with_columns([
            pl.when(pl.col("daily_change_pct") > 2)
                .then(pl.lit("🔥 Forte Alta"))
                .when(pl.col("daily_change_pct") > 1)
                .then(pl.lit("⚡ Em Alta"))
                .when(pl.col("daily_change_pct") < -2)
                .then(pl.lit("❄️ Forte Baixa"))
                .when(pl.col("daily_change_pct") < -1)
                .then(pl.lit("📉 Em Baixa"))
                .otherwise(pl.lit("➡️ Estável"))
                .alias("nivel_desempenho")
            ])
            .select([
            "code_exchange_rate",
            "exchange_rate", 
            "daily_change_pct",
            "weekly_change_pct", 
            "monthly_change_pct",
            "nivel_desempenho"
            ])
            .sort("daily_change_pct", descending=True)
        )

        # Análise de volatilidade por moeda
        df_volatility = (df_performance
            .group_by("code_exchange_rate")
            .agg([
                pl.col("daily_change_pct").cast(pl.Float64).std().round(2).alias("volatility_30d"),
                pl.col("daily_change_pct").cast(pl.Float64).mean().round(2).alias("avg_change_30d"),
                pl.col("daily_change_pct").cast(pl.Float64).max().round(2).alias("max_gain_30d"),
                pl.col("daily_change_pct").cast(pl.Float64).min().round(2).alias("max_loss_30d"),
                pl.col("exchange_rate").len().alias("days_tracked")
            ])
            .with_columns([
                pl.when(pl.col("volatility_30d") > 3)
                    .then(pl.lit("🔴 Alto"))
                    .when(pl.col("volatility_30d") > 1.5)
                    .then(pl.lit("🟡 Médio"))
                    .otherwise(pl.lit("🟢 Baixo"))
                    .alias("risk_score")
            ])
            .sort("volatility_30d", descending=True)
        )

        path_sink = "data/gold/finance/currency_heatmap.parquet"
        self.polars_write_parquet_overwrite(df_heatmap, path_sink)

        path_sink = "data/gold/finance/top_gainers.parquet"
        self.polars_write_parquet_overwrite(top_gainers, path_sink)

        path_sink = "data/gold/finance/top_losers.parquet"
        self.polars_write_parquet_overwrite(top_losers, path_sink)

        path_sink = "data/gold/finance/volatility_exchange_rates.parquet"
        self.polars_write_parquet_overwrite(df_volatility, path_sink)


if __name__ == "__main__":
    gold = Gold()
    time_a = time()
    gold.execute()
    print("Executado em:", time() - time_a)
