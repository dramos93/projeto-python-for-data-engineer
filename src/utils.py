class PolarsProcessor:
    def polars_read_parquet(self):
        print("Dado lidos.")
        pass

    def polars_filter_new_records(self):
        print("Dados novos encontrados.")

    def polars_write_parquet(self):
        self.polars_filter_new_records()
        print("Dados gravados com sucesso.")
        pass
