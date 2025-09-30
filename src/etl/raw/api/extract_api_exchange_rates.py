from requests import get
from dotenv import load_dotenv
import os

# from tenacity import retry, stop_after_attempt, wait_fixed
from pathlib import Path
from datetime import datetime
import json

load_dotenv()


def get_data_from_exchange_rates_api():
    API_KEY = os.getenv("API_KEY")
    if not API_KEY:
        raise ValueError("API_KEY não encontrada nas variáveis de ambiente.")

    url = f"https://v6.exchangerate-api.com/v6/{API_KEY}/latest/BRL"
    response = get(url)
    if response.status_code != 200:
        print(response.text)
    response.raise_for_status()
    return response.json()


def save_json_file_raw(json_file: dict, path_raw: str):
    raw_path = Path(path_raw)
    raw_path.mkdir(parents=True, exist_ok=True)

    # nome do arquivo pode ter timestamp da ingestão
    file_name = raw_path / f"exchange_rates_{datetime.now().date().today()}.json"
    with open(file_name, "w") as f:
        json.dump(json_file, f, indent=4)
    print("Salvo em: ", file_name)


if __name__ == "__main__":
    data_json = get_data_from_exchange_rates_api()
    save_json_file_raw(data_json, "data/raw/api/exchange_rates")
