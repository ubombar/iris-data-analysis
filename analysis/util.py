import asyncio
from config import Config
import httpx
import requests
import io 
import json
import pandas as pd 
from io import StringIO

class QueryClient():
    def __init__(self, cfg: Config):
        self.cfg = cfg

    def get_params(self, query: str):
        return {
            "enable_http_compression": "false",
            "default_format": self.cfg.response_format,
            "output_format_json_quote_64bit_integer": "",
            "database": "iris",
            "query": query,
        }

    def get_headers(self):
        return {
            "User-Agent": "irisctl",
            "Accept": "application/json",
            "Authorization": self.cfg.authorization_token,
        }
    
    async def async_query_raw(self, query: str):
        '''Returns a Generator which returns the chunk as raw bytes'''
        params = self.get_params(query)
        headers = self.get_headers()

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream("POST", url=self.cfg.url, headers=headers, params=params, timeout=self.cfg.timeout) as response:
                    # Check for ant errors
                    response.raise_for_status()

                    async for chunk in response.aiter_bytes(chunk_size=self.cfg.chunk_size):
                        # Skip for keepalive messages
                        if not chunk: continue

                        # yield the chunk
                        yield chunk

        except httpx.RequestError as e:
            print(f'Error while async_query_raw:{e}')
    
    async def async_query_lines(self, query: str):
        '''Returns a Generator which returns the line as string'''
        params = self.get_params(query)
        headers = self.get_headers()

        try:
            async with httpx.AsyncClient() as client:
                async with client.stream("POST", url=self.cfg.url, headers=headers, params=params, timeout=self.cfg.timeout) as response:
                    # Check for ant errors
                    response.raise_for_status()

                    async for chunk in response.aiter_lines():
                        # Skip for keepalive messages
                        if not chunk: continue

                        # yield the chunk
                        yield chunk

        except httpx.RequestError as e:
            print(f'Error while async_query_lines:{e}')

    async def async_query_getall_bytes(self, query: str):
        '''Returns the all of the bytes'''
        buffer = io.BytesIO()

        async for line in self.async_query_raw(query):
            buffer.write(line)
        
        val = buffer.getvalue()
        buffer.close()

        return val

    async def async_query_getall_str(self, query: str, format_it: str=None, print_it: str=None, **kwargs):
        '''Returns the all of the str'''
        buffer = []

        async for line in self.async_query_lines(query):
            buffer.append(line)
            buffer.append("\n") # add new line 

        val = "".join(buffer)

        if print_it == 'json':
            assert self.cfg.response_format in ["JSON", "JSONCompact"], f"format is not supported '{format_it}'"
            self.print_json(val, **kwargs)

        if format_it == 'json':
            return self.format_json(val, **kwargs)
        elif format_it == 'df':
            assert self.cfg.response_format in ["CSVWithNames", "CSV"], f"format is not supported '{format_it}'"
            return self.format_df(val, **kwargs, headless=self.cfg.response_format == 'CSVWithNames')

        return val 
    
    def print_json(self, resp: str, indent=1):
        try:
            print(json.dumps(json.loads(resp), indent=indent))
        except:
            print("This is not a valid json")

    def format_json(self, resp: str):
        try:
            return json.loads(resp)
        except:
            print("This is not a valid json")

    def format_df(self, resp: str, headless: bool=True):
        try:
            return pd.read_csv(StringIO(resp), header=None if headless else 'infer')
        except:
            print("This is not a valid json")