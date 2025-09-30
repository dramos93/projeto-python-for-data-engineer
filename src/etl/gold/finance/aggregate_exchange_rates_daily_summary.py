from src.utils import PolarsProcessor
from time import time
import polars as pl


class Gold(PolarsProcessor):
    def __init__(self):
        super().__init__()

    def execute(self):
        source_path = "data/silver/finance/exchange_rates"
        # path_sink = "data/gold/finance/exchange_rates"
        lazy_df = self.polars_read_parquet(source_path)
        # self.polars_write_parquet(lazy_df, path_sink)
        print(lazy_df.collect())
        print(lazy_df.collect().select(pl.count()))

if __name__ == "__main__":
    gold = Gold()
    time_a = time()
    gold.execute()


    print(time() - time_a)