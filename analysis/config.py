import os 
from dotenv import load_dotenv
import httpx

# First thing is to load the environment variables.
load_dotenv()

class Config():
    def __init__(self, **kwargs):
        self.url: str = "https://chproxy.iris.dioptra.io/" # change this to "https://chproxy.iris-research.dioptra.io/" for the test instance.
        self.database: str = "iris"
        self.chunk_size: int = 1 * 1024 * 1024
        self.user_agent: str = "client"
        self.authorization_token: str = os.environ["IRIS_TOKEN"]
        self.timeout: httpx.Timeout = httpx.Timeout(None)
        
        for k, v in kwargs.items():
            setattr(self, k, v)
