from src.utils import PolarsProcessor
from time import time
import polars as pl
from rich import print
from datetime import date


class Gold(PolarsProcessor):
    def __init__(self):
        super().__init__()

    def execute(self):
        source_path = "data/silver/finance/exchange_rates"
        lazy_df = self.polars_read_parquet(source_path)

        df_heatmap = lazy_df.with_columns(
            [
                # Valor anterior (lag de 1)
                pl.col("exchange_rate")
                .shift(1)
                .over(["code_exchange_rate"])
                .sort_by("time_last_update_utc")
                .alias("prev_rate"),
                # Variação diária em porcentagem
                (
                    (
                        (
                            pl.col("exchange_rate")
                            - pl.col("exchange_rate")
                            .shift(1)
                            .over(["code_exchange_rate"])
                            .sort_by("time_last_update_utc")
                        )
                        / pl.col("exchange_rate")
                        .shift(1)
                        .over(["code_exchange_rate"])
                        .sort_by("time_last_update_utc")
                        * 100
                    )
                    .round(2)
                    .alias("daily_change_pct")
                ),
                # Variação semanal em porcentagem (lag de 7)
                (
                    (
                        (
                            pl.col("exchange_rate")
                            - pl.col("exchange_rate")
                            .shift(7)
                            .over(["code_exchange_rate"])
                            .sort_by("time_last_update_utc")
                        )
                        / pl.col("exchange_rate")
                        .shift(7)
                        .over(["code_exchange_rate"])
                        .sort_by("time_last_update_utc")
                        * 100
                    )
                    .round(2)
                    .alias("weekly_change_pct")
                ),
                # Variação mensal em porcentagem (lag de 30)
                (
                    (
                        (
                            pl.col("exchange_rate")
                            - pl.col("exchange_rate")
                            .shift(30)
                            .over(["code_exchange_rate"])
                            .sort_by("time_last_update_utc")
                        )
                        / pl.col("exchange_rate")
                        .shift(30)
                        .over(["code_exchange_rate"])
                        .sort_by("time_last_update_utc")
                        * 100
                    )
                    .round(2)
                    .alias("monthly_change_pct")
                ),
            ]
        )
        df_heatmap_agg = (
            df_heatmap.filter(
            pl.col("time_last_update_utc").cast(pl.Date) == date.today()
            )
            .select(
            [
                pl.col("time_last_update_utc").cast(pl.Date),
                "code_exchange_rate",
                "exchange_rate",
                "daily_change_pct",
                "weekly_change_pct",
                "monthly_change_pct",
            ]
            )
            .with_columns(
                pl.when(pl.col("daily_change_pct") > 2)
                .then(pl.lit("🔥 Alta Expressiva"))
                .when(pl.col("daily_change_pct") > 1)
                .then(pl.lit("📈 Alta Moderada"))
                .when(pl.col("daily_change_pct") < -2)
                .then(pl.lit("📉 Baixa Expressiva"))
                .when(pl.col("daily_change_pct") < -1)
                .then(pl.lit("⬇️ Baixa Moderada"))
                .otherwise(pl.lit("➡️ Estável"))
                .alias("performance_tier")
            )
        )
        print(df_heatmap_agg.collect_schema())
        path_sink = "data/gold/finance/heatmap_agg.parquet"
        self.polars_write_parquet_overwrite(df_heatmap_agg, path_sink)


if __name__ == "__main__":
    gold = Gold()
    time_a = time()
    gold.execute()
    print("Executado em:", time() - time_a)
