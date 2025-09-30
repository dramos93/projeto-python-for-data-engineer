import polars as pl
from time import time
from typing import List


class PolarsProcessor:
    def generate_hash_column(
        self, hash_columns: List[str], hash_column_name: str = "hash_diff"
    ) -> pl.Expr:
        """
        Creates a Polars expression to generate a SHA-256 hash from the concatenation of several columns.

        This function utilizes the .chash namespace provided by the polars_hash library.

        Args:
            hash_columns: A list of strings with the names of the columns to be hashed.
            hash_column_name: The name of the new column that will contain the hash.

        Returns:
            A Polars expression that can be used within `with_columns`.
        """
        # 1. Create a list of expressions to prepare each column
        #    (cast to String and fill nulls)
        column_expressions = [
            pl.col(c).cast(pl.String).fill_null("") for c in hash_columns
        ]

        # 2. Concatenate all prepared columns, then compute the hash using the .chash
        #    namespace from the polars_hash library, and finally alias the result.
        hash_expression = (
            pl.concat_str(column_expressions).chash.sha2_256().alias(hash_column_name)
        )

        return hash_expression

    def polars_read_json(self, path):
        return pl.read_json(path).lazy()
    
    def polars_read_parquet(self, path):
        return pl.scan_parquet(path)

    def polars_filter_new_records(
        self, new_lazy_df, path_sink, subset_cols="hash_diff"
    ):
        try:
            old_data = pl.scan_parquet(f"{path_sink}/*.parquet")
            # Se o diretório existe, mas está vazio, o scan pode não falhar,
            # mas o DataFrame estará vazio.
            if old_data.collect().is_empty():
                return new_lazy_df

            # Realiza o anti-join para manter apenas os registros em new_lazy_df
            # que não existem em old_data, com base nas colunas chave.
            new_records_lazy_df = new_lazy_df.join(old_data, on=subset_cols, how="anti")
            return new_records_lazy_df

        except (
            Exception
        ):  # Idealmente, capturar pl.exceptions.ComputeError ou FileNotFoundError
            # Se o path_sink não existe, todos os dados são novos.
            return new_lazy_df

    def polars_write_parquet(self, lazy_df, path_sink, key_cols="hash_diff"):
        lazy_df_to_write = self.polars_filter_new_records(lazy_df, path_sink, key_cols)
        if not lazy_df_to_write.first().collect().is_empty():
            sink_path = f"{path_sink}/part-{int(time() * 1000)}.parquet"
            lazy_df_to_write.sink_parquet(sink_path, compression="snappy")
        else:
            print("Nenhum registro novo foi encontrado para salvar.")
