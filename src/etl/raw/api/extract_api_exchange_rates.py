from requests import get
# from tenacity import retry, stop_after_attempt, wait_fixed
from pathlib import Path
from datetime import datetime
import json



def get_data_from_exchange_rates_api():
    API_KEY = "554d54e2d67c27e8a1399fe4"
    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/BRL"
    response = get(url)
    if response.status_code != 200:
        print(response.text)
    response.raise_for_status()
    return response.json()

def save_json_file_raw(json_file: dict, path_raw : str = None):
    # diretório raw, organizado por ano/mês/dia
    path_raw = "data/raw/api/exchange_rates"
    raw_path = Path(path_raw)
    raw_path.mkdir(parents=True, exist_ok=True, mode=0o777)

    # nome do arquivo pode ter timestamp da ingestão
    file_name = raw_path / f"exchange_rates_{datetime.now().date().today()}.json"
    print(json_file.keys())
    with open(file_name, "w") as f:
        json.dump(json_file, f, indent=4)

if __name__ == "__main__":
    data_json = get_data_from_exchange_rates_api()
    save_json_file_raw(data_json, "data/raw/api/exchange_rates")


    
