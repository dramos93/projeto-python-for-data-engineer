import polars as pl
import polars.selectors as cs
from datetime import datetime
from rich import print
from time import time
from src.utils import PolarsProcessor

class Transform(PolarsProcessor):
    def __init__(self):
        super().__init__()

    def get_table(self, path):
        lazy_df = pl.read_json(path).lazy()
        return lazy_df

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
                on=pl.exclude([*index_cols, "conversion_rates"]), # Adicionado "conversion_rates" para garantir que a coluna original não entre
                variable_name="code_exchange_rate",
                value_name="exchange_rate",
            )
            
            # 4. Remove a coluna struct original que não é mais necessária
            # .drop("conversion_rates")

            # 5. Filtra valores nulos (se houver)
            .filter(pl.col("exchange_rate").is_not_null())
        )
        lazy_df = lazy_df.with_columns(pl.lit("api").alias("type_src"))
        return lazy_df

    def write_data(self, lazy_df):
        sink_path = f"data/silver/finance/exchange_rates/part-{int(time()*1000)}.parquet"
        self.polars_read_parquet()
        self.polars_write_parquet()
        lazy_df.sink_parquet(sink_path, compression="snappy", mkdir=True)

    def execute(self):
        today = datetime.now().date().today()
        source_path = f"data/raw/api/exchange_rates/exchange_rates_{today}.json"
        lazy_df = self.get_table(source_path)
        silver_df = self.transformations(lazy_df)
        self.write_data(silver_df)


if __name__ == "__main__":
    time_a = time()
    tranform_exchange_rates = Transform()
    tranform_exchange_rates.execute()
    print(time() - time_a)