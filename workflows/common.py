import asyncio
from config import Config
import httpx
import requests

async def run_query(cfg: Config, query: str):
    params = {
        "enable_http_compression": "false",
        "default_format": cfg.response_format,
        "output_format_json_quote_64bit_integer": "",
        "database": "iris",
        "query": query,
    }

    headers = {
        "User-Agent": "irisctl",
        "Accept": "application/json",
        "Authorization": cfg.authorization_token,
    }

    try:
        async with httpx.AsyncClient() as client:
            async with client.stream("POST", url=cfg.url, headers=headers, params=params, timeout=cfg.timeout) as response:
                # Check for ant errors
                response.raise_for_status()
                async for chunk in response.aiter_bytes(chunk_size=cfg.chunk_size):
                    # Skip for keepalive messages
                    if not chunk: continue

                    # yield the chunk
                    yield chunk

    except httpx.RequestError as e:
        print("An error eccured during the download:", e)
        yield e 

async def run_query_iter_lines(cfg: Config, query: str):
    '''USe CSV or JSONEachRow data format'''
    params = {
        "enable_http_compression": "false",
        "default_format": cfg.response_format,
        "output_format_json_quote_64bit_integer": "",
        "database": "iris",
        "query": query,
    }

    headers = {
        "User-Agent": "irisctl",
        "Accept": "application/json",
        "Authorization": cfg.authorization_token,
    }

    try:
        async with httpx.AsyncClient() as client:
            async with client.stream("POST", url=cfg.url, headers=headers, params=params, timeout=cfg.timeout) as response:
                # Check for ant errors
                response.raise_for_status()
                async for chunk in response.aiter_bytes(chunk_size=cfg.chunk_size):
                    # Skip for keepalive messages
                    if not chunk: continue

                    # yield the chunk
                    yield chunk

    except httpx.RequestError as e:
        print("An error eccured during the download:", e)

async def quick_query(cfg: Config, q: str):
    buffer = b''
    async for chunk in run_query(cfg, q):
        buffer = buffer + chunk
    return buffer