import polars as pl

# import polars.selectors as cs
from datetime import datetime

# from rich import print
# from time import time
from src.utils import PolarsProcessor
import polars_hash as plh
# from typing import List


class Transform(PolarsProcessor):
    def __init__(self):
        super().__init__()

    def transformations(self, lazy_df):
        # Define as colunas que servirão de identificadores (não serão pivotadas)
        index_cols = [
            "result",
            "documentation",
            "terms_of_use",
            "time_last_update_unix",
            "time_last_update_utc",
            "time_next_update_unix",
            "time_next_update_utc",
            "base_code",
        ]

        # 1. Inicia uma varredura "lazy" do arquivo JSON com pl.scan_json
        lazy_df = (
            lazy_df
            # 2. Desaninha (unnest) a coluna struct "conversion_rates" de forma automática
            .with_columns(pl.col("conversion_rates").struct.unnest())
            # 3. Usa unpivot (o substituto de melt) para transformar de formato largo para longo.
            #    pl.exclude() seleciona todas as colunas EXCETO as de índice,
            #    sem precisar saber os nomes das moedas de antemão.
            .unpivot(
                index=index_cols,
                on=pl.exclude(
                    [*index_cols, "conversion_rates"]
                ),  # Adicionado "conversion_rates" para garantir que a coluna original não entre
                variable_name="code_exchange_rate",
                value_name="exchange_rate",
            )
            # 4. Remove a coluna struct original que não é mais necessária
            # .drop("conversion_rates")
            # 5. Filtra valores nulos (se houver)
            .filter(pl.col("exchange_rate").is_not_null())
            .with_columns(
                pl.lit("api").alias("type_src"),
                pl.lit("exchangerate-api").alias("system_src"),
                (
                    pl.col("time_last_update_utc")
                    .str.strptime(pl.Datetime, "%a, %d %b %Y %H:%M:%S %z", strict=False)
                    .cast(pl.Datetime)
                    .alias("time_last_update_utc")
                ),
                (
                    pl.col("time_next_update_utc")
                    .str.strptime(pl.Datetime, "%a, %d %b %Y %H:%M:%S %z", strict=False)
                    .cast(pl.Datetime)
                    .alias("time_next_update_utc")
                ),
                pl.col("exchange_rate").cast(pl.Decimal(20, 4)),
                pl.lit(datetime.now()).alias("load_date_src"),
            )
            .with_columns(
                self.generate_hash_column(
                    index_cols
                    + [
                        "code_exchange_rate",
                        "exchange_rate",
                        "type_src",
                        "system_src",
                    ]
                )
            )
        )
        return lazy_df

    def execute(self):
        today = datetime.now().date().today()
        source_path = f"data/raw/api/exchange_rates/exchange_rates_{today}.json"

        # import os
        # paths = os.listdir("data/raw/api/exchange_rates/")
        # for path in paths:
        #     path_sink = "data/silver/finance/exchange_rates"
        #     lazy_df = self.polars_read_json("data/raw/api/exchange_rates/" + path)
        #     # print(lazy_df.select(pl.len()).collect())
        #     silver_df = self.transformations(lazy_df)
        #     self.polars_write_parquet_append(silver_df, path_sink)

        path_sink = "data/silver/finance/exchange_rates"
        lazy_df = self.polars_read_json(source_path)
        silver_df = self.transformations(lazy_df)
        self.polars_write_parquet_append(silver_df, path_sink)


if __name__ == "__main__":
    tranform_exchange_rates = Transform()
    tranform_exchange_rates.execute()
