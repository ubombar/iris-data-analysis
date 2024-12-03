import requests
import pandas as pd 
from io import StringIO
import os 
from dotenv import load_dotenv
from typing import Literal

# First thing is to load the environment variables.
load_dotenv()

class Config():
    def __init__(self, **kwargs):
        self.url: str = "https://chproxy.iris.dioptra.io/" # change this to "https://chproxy.iris-research.dioptra.io/" for the test instance.
        self.database: str = "iris"
        self.chunk_size: int = 1 * 1024 * 1024
        self.user_agent: str = "client"
        self.authorization_token: str = os.environ["IRIS_TOKEN"]
        self.timeout: int = None
        
        for k, v in kwargs.items():
            setattr(self, k, v)

ResponseFormat = Literal["CSVWithNames", "JSON", "JSONCompact", "CSV", "RowBinary"]

class IrisClickhouseClient():
    def __init__(self, cfg: Config=Config()):
        self.cfg = cfg

    def get_params(self, query: str, response_format: ResponseFormat="CSVWithNames"):
        return {
            "enable_http_compression": "false",
            "default_format": response_format,
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
    
    # async def query_stream_raw(self, query: str, response_format: ResponseFormat="CSVWithNames"):
    #     '''Returns a Generator which returns the chunk as raw bytes'''
    #     params = self.get_params(query, response_format=response_format)
    #     headers = self.get_headers()

    #     try:
    #         async with httpx.AsyncClient() as client:
    #             async with client.stream("POST", url=self.cfg.url, headers=headers, params=params, timeout=self.cfg.timeout) as response:
    #                 # Check for ant errors
    #                 response.raise_for_status()

    #                 async for chunk in response.aiter_bytes(chunk_size=self.cfg.chunk_size):
    #                     # Skip for keepalive messages
    #                     if not chunk: continue

    #                     # yield the chunk
    #                     yield chunk

    #     except httpx.RequestError as e:
    #         print(f'Error while async_query_raw:{e}')
    
    # async def query_stream_lines(self, query: str, response_format: ResponseFormat="CSVWithNames"):
    #     '''Returns a Generator which returns the line as string'''
    #     params = self.get_params(query, response_format=response_format)
    #     headers = self.get_headers()

    #     try:
    #         async with httpx.AsyncClient() as client:
    #             async with client.stream("POST", url=self.cfg.url, headers=headers, params=params, timeout=self.cfg.timeout) as response:
    #                 # Check for ant errors
    #                 response.raise_for_status()

    #                 async for chunk in response.aiter_lines():
    #                     # Skip for keepalive messages
    #                     if not chunk: continue

    #                     # yield the chunk
    #                     yield chunk

    #     except httpx.RequestError as e:
    #         print(f'Error while async_query_lines:{e}')

    # async def query_bytes(self, query: str, response_format: ResponseFormat="CSVWithNames"):
    #     '''Returns the all of the bytes'''
    #     buffer = io.BytesIO()

    #     async for line in self.query_stream_raw(query, response_format=response_format):
    #         buffer.write(line)
        
    #     val = buffer.getvalue()
    #     buffer.close()

    #     return val

    # async def query_str(self, query: str, response_format: ResponseFormat="CSVWithNames", format_it: str=None, print_it: str=None, **kwargs):
    #     '''Returns the all of the str'''
    #     buffer = []

    #     async for line in self.query_stream_lines(query, response_format=response_format):
    #         buffer.append(line)
    #         buffer.append("\n") # add new line 

    #     val = "".join(buffer)

    #     if print_it == 'json':
    #         assert response_format in ["JSON", "JSONCompact"], f"format is not supported '{format_it}'"
    #         self.print_json(val, **kwargs)

    #     if format_it == 'json':
    #         return self.format_json(val, **kwargs)
    #     elif format_it == 'df':
    #         assert response_format in ["CSVWithNames", "CSV"], f"format is not supported '{format_it}'"
    #         return self.format_df(val, **kwargs, headless=response_format == 'CSVWithNames')

    #     return val 
    
    # def print_json(self, resp: str, indent=1):
    #     try:
    #         print(json.dumps(json.loads(resp), indent=indent))
    #     except:
    #         print("This is not a valid json")

    # def format_json(self, resp: str):
    #     try:
    #         return json.loads(resp)
    #     except:
    #         print("This is not a valid json")

    # def format_df(self, resp: str, headless: bool=True):
    #     try:
    #         return pd.read_csv(StringIO(resp), header=None if headless else 'infer')
    #     except:
    #         print("This is not a valid csv")

    def query_df(self, query: str):
        '''Runs the query and retuns a pandas df'''
        params = self.get_params(query, response_format="CSVWithNames")
        headers = self.get_headers()

        try:
            with requests.post(self.cfg.url, headers=headers, params=params, timeout=None) as response:
                    # Check for ant errors
                    response.raise_for_status()

                    in_memory_buffer = StringIO(response.text)

                    return pd.read_csv(in_memory_buffer, header='infer')

        except Exception as e:
            print(f'Error while query:{e}')