# External imports
import pandas as pd 
from dataclasses import dataclass
import json
from datetime import datetime
from tqdm import tqdm
import asyncio
import os
import ipaddress
import multiprocessing
import io 

# Internal imports
from config import config, Config
import common
import file as ff
import objects

# This is the output of table_downloader
input_tables_name = "../data/measurements/measurement-2024-11-18 22:44:16.feather"
selected_measurement_uuid = "007046a9_518e_46cb_8e70_e598b8bce831"

measurement_tables_df = pd.read_feather(input_tables_name)

selected_measurements_df = measurement_tables_df[
    (measurement_tables_df['cleaned'] == True) & 
    (measurement_tables_df['measurement_uuid'] == selected_measurement_uuid) & 
    (measurement_tables_df['type'] == 'results')
]

# Convert the df to list[dataclass: Measurement]
selected_measurements_list = [objects.Measurement(**row) for _, row in selected_measurements_df.iterrows()]
print(f"There are {len(selected_measurements_list)} table(s).")

async def get_number_of_row(m: objects.Measurement): 
    q_size = f"select count(*) from {m.table_name}"
    cfg = Config()
    cfg.response_format = "CSV"
    raw_response = await common.quick_query(cfg, q_size)
    return int(raw_response.decode())

def bytes_consumer(buffer_bytes: bytes):
    row_size = 32
    ipv6_size = 16
    new_row_size = 6
    num_pairs = len(buffer_bytes) // row_size # each row is 32 bytes
    new_buffer_size = num_pairs * new_row_size # Each row will be 6 bytes in the new one

    new_buffer = io.BytesIO()
    new_buffer.truncate(new_buffer_size)

    offset = 0
    for _ in range(num_pairs):
        row = buffer_bytes[offset: offset + row_size]
        
        first = buffer_bytes[offset:offset + ipv6_size]
        second = buffer_bytes[offset + ipv6_size:offset + row_size]

        first_address = ipaddress.ip_address(first).ipv4_mapped
        second_address = ipaddress.ip_address(second).ipv4_mapped

        if first_address == None or second_address == None:
            print("Invalid IPv6 Addresses, this might be a problem.")

        new_buffer.write(first_address.packed[:-1])
        new_buffer.write(second_address.packed[:-1])

        offset += row_size

    new_buffer.seek(0)
    return new_buffer

async def get_results_table_optimized(m: objects.Measurement, num_rows: int, limit: int=1, pbar: tqdm=None, pbar_update=100):
    # Define the packing/compression function
    def from_csv_to_int(binary_data: str) -> tuple[bytes, bool]:
        if not isinstance(binary_data, str):
            raise Exception("Given wrong type!")
        
        a, b = binary_data.split(',')
        a, b = a[1:-1], b[1:-1]
        a, b = ipaddress.IPv6Address(a).ipv4_mapped, ipaddress.IPv6Address(b).ipv4_mapped

        # remove the last byte since it is a prefix. The reouting information is represented with 
        # 3 + 3 = 6 bytes.
        return a.packed[:-1] + b.packed[:-1], True

    # Get the default config
    cfg = Config()
    cfg.response_format = "RowBinary"
    cfg.chunk_size = 32 * 1024 # Make the chunks alighed with 2 ipb6 addresses (should be divisible by 32, so 32 Kb)

    # Set the quert string
    q = f"select probe_dst_prefix, reply_src_prefix from {m.table_name}{'' if limit == 0 else ' limit ' + str(limit)}"

    # Determine the name of the binary data
    filepath = f"../data/ip_data/{m.measurement_uuid}-{ff.get_timestr()}/{m.table_name}.bin"
    num_pairs_written = 0
    num_pairs_expected = num_rows

    num_bytes_downloaded = 0
    
    try:
        with ff.BinaryFile(filepath, dummy=False) as bf:
            async for chunk in common.run_query(cfg, q):
                # b, ok = from_csv_to_int(row)
                # if not ok: continue
                # num_pairs_written += 1

                # pbar.update(6 / (1024*1024)) # one row is 6 bytes
                if num_pairs_written % pbar_update == 0: 
                    pbar.update(num_bytes_downloaded / (1024*1024))
                    num_bytes_downloaded = 0

                in_memory_bytesio = bytes_consumer(chunk)

                bf.write(in_memory_bytesio.read())

                in_memory_bytesio.close() # close the bytesio object

                num_bytes_downloaded += len(chunk)
    except Exception as e:
        print(f"Exception on {m.table_name}, exitting")
        return (False, num_pairs_expected, num_pairs_written)

    return (True, num_pairs_expected, num_pairs_written)

async def get_results_table_optimized_from_list(m_list: list[objects.Measurement], limit: int=1):
    num_rows_list = await asyncio.gather(*[get_number_of_row(m) for m in m_list])
    if limit == 0:
        num_rows_c = sum(num_rows_list)
        num_rows_tqdm = num_rows_c
    else:
        num_rows_c = limit * len(m_list)
        num_rows_tqdm = limit * len(m_list)

    # One row is 6 bytes
    # each row is 2 ipv6 addresses = 32 bytes
    total_mb = round((32 * num_rows_c) / (1024*1024), ndigits=2)
    print(f"Will download total of {total_mb} row(s) and {total_mb} MB.")

    pbar = tqdm(total=total_mb, desc="Downloading", unit="MB")

    r = await asyncio.gather(*[
        get_results_table_optimized(m, nr, limit=limit, pbar=pbar) for m, nr in zip(m_list, num_rows_list)
        ])
    pbar.close()
    return r

    # with multiprocessing.Pool(processes=len(m_list)) as p:
    #     p.starmap()

async def main():
    ok_expected_writted_list = await get_results_table_optimized_from_list(selected_measurements_list, limit=1_000_000)

    for m, ok_exp_writ in zip(selected_measurements_list, ok_expected_writted_list):
        ok, expected, written = ok_exp_writ
        print(f"For agent {m.agent_uuid} {written}/{expected} pairs are saved with {'success' if ok else 'error'}")

    # await get_results_table_optimized(selected_measurements_list[0], 1, limit=1)

if __name__ == "__main__":
    asyncio.run(main())