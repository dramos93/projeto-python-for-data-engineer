import polars as pl
import polars.selectors as cs
from datetime import datetime
class Transform():
    def load_data(self):
        today = datetime.now().today().date()
        df_raw = pl.read_json(f"data/raw/api/exchange_rates/exchange_rates_{today}.json")
        conversion_rates_fields = df_raw.schema["conversion_rates"].to_schema().keys()
        df_raw = df_raw.with_columns(
            [pl.col("conversion_rates").struct.field(field)
             for field
             in conversion_rates_fields]
            )

        df_raw = df_raw.melt(
            id_vars=[
                "result",
                "documentation",
                "terms_of_use",
                "time_last_update_unix",
                "time_last_update_utc",
                "time_next_update_unix",
                "time_next_update_utc",
                "base_code",
            ],
            value_vars=conversion_rates_fields,
            variable_name="code",
            value_name="value"
        )
        print(df_raw.filter(df_raw["code"] == "USD"))
        return df_raw


    def transform_data(self):
        print("Transform Data.")


    def execute(self):
        self.load_data()

if __name__ == "__main__":
    tranform_exchange_rates = Transform()
    tranform_exchange_rates.execute()