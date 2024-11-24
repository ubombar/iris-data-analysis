import os 
from dotenv import load_dotenv
import httpx

# First thing is to load the environment variables.
load_dotenv()

class Config():
    url: str = "https://chproxy.iris.dioptra.io/" # change this to "https://chproxy.iris-research.dioptra.io/" for the test instance.
    database: str = "iris"
    response_format: str = "CSVWithNames"
    chunk_size: int = 1024 * 1024
    user_agent: str = "client"
    authorization_token: str = os.environ["IRIS_TOKEN"]
    timeout: httpx.Timeout = httpx.Timeout(None)

# Make an instance that will be used globally.
config = Config()