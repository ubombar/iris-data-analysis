import numpy as np
import pandas 
import requests
from tqdm import tqdm
from dataclasses import dataclass
import datetime
import pandas as pd 

def download_query_result(output_filename: str, query: str, token: str, default_format: str = "CSVWithNames", chunk_size: int = 1024*1024):
    url = "https://chproxy.iris.dioptra.io/"
    params = {
        "enable_http_compression": "false",
        "default_format": default_format,
        "output_format_json_quote_64bit_integer": "",
        "database": "iris",
        "query": query,
    }

    # Headers
    headers = {
        "User-Agent": "irisctl",
        "Accept": "application/json",
        "Authorization": token,
    }

    try:
        with requests.post(url, headers=headers, params=params, stream=True) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            with open(output_filename, "wb") as file:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if not chunk:
                        continue # filter out keep alives
                    file.write(chunk)
    except requests.exceptions.RequestException as e:
        print("an error occured during the download.")


def download_table_csv(output_filename: str, table_name: str, token: str, limit: int=0, chunk_size: int = 1024*1024):
    url = "https://chproxy.iris.dioptra.io/"
    limit_query = f"limit {limit}" if limit > 0 else ""
    params = {
        "enable_http_compression": "false",
        "default_format": "CSVWithNames",
        "output_format_json_quote_64bit_integer": "",
        "database": "iris",
        "query": f"select * from {table_name} {limit_query}"
    }

    # Headers
    headers = {
        "User-Agent": "irisctl",
        "Accept": "application/json",
        "Authorization": token,
    }

    try:
        with requests.post(url, headers=headers, params=params, stream=True) as response:
            response.raise_for_status()
            total_size = int(response.headers.get('content-length', 0))
            with open(output_filename, "wb") as file:
                with tqdm(total=total_size, unit="B", unit_scale=True, desc="Downloading") as pbar:
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if not chunk:
                            continue # filter out keep alives
                        file.write(chunk)
                        pbar.update(len(chunk))
    except requests.exceptions.RequestException as e:
        print("an error occured during the download.")


def convert_to_measurement(table_names: list[str]) -> pd.DataFrame:
    @dataclass
    class MeasurementTable():
        measurement_uuid: str 
        type: str 
        cleaned: bool 
        prefix: str 
        agent_uuid: str 
        table_name: str 
        retrieved: datetime

    measurement_tables = []

    for table_name in table_names:
        split_array = table_name.split("__")
        if len(split_array) != 3:
            continue

        prefix, measurement_uuid, agent_uuid = split_array
        
        measurement_tables.append(MeasurementTable(
            measurement_uuid=measurement_uuid,
            agent_uuid=agent_uuid,
            prefix=prefix,
            cleaned="clean" in prefix,
            table_name=table_name,
            type=prefix.replace("cleaned_", ""),
            retrieved=datetime.now(),
        ))

    meas_df = pd.DataFrame(measurement_tables)
    return meas_df