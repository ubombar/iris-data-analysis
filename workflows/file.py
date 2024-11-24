import os 
import random
import hashlib
from datetime import datetime
import pandas as pd 

def generate_random_hash_str(prefix: str="", postfix: str=""):
    h = hashlib.sha256(str(datetime.now()).encode("utf-8")).hexdigest()
    return f"{prefix}{h}{postfix}"

def get_timestr(dt: datetime=datetime.now()):
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def save_dataframe_feather(df: pd.DataFrame, foldername: str, prefix: str="", postfix: str=""):
    os.makedirs(foldername, exist_ok=True)
    time_str = get_timestr()
    filename = os.path.join(foldername, f"{prefix}{time_str}{postfix}.feather")
    df.to_feather(filename)

class TemporaryFile:
    def __init__(self):
        self.filename = os.path.join("/tmp/", generate_random_hash_str(prefix="iris-"))
        self.file = None
        
    def __enter__(self):
        self.file = open(self.filename, "wb+") # Opening for writing in binary and seekable.
        return self
    
    def reset(self):
        self.file.seek(0)
    
    def close(self):
        if not self.file.closed:
            self.file.close()
    
    def delete(self):
        if os.path.exists(self.filename):
            os.remove(self.filename)
    
    def write(self, b: bytes):
        self.file.write(b)

    def __exit__(self, exc_type, exc_value, _):        
        # Handle exceptions if needed (optional)
        if exc_type:
            print(f"An exception occurred: {exc_type}, {exc_value}")

        self.close()
        self.delete()
        
        return True
    
class BinaryFile():
    def __init__(self, filepath: str, create_parent: bool=True):
        if create_parent:
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            
        self.filepath = filepath
        self.file = None
        
    def __enter__(self):
        self.file = open(self.filepath, "wb+") # Opening for writing in binary and seekable.
        return self
    
    def write(self, b: bytes):
        self.file.write(b)

    def __exit__(self, exc_type, exc_value, _):        
        # Handle exceptions if needed (optional)
        if exc_type:
            print(f"An exception occurred: {exc_type}, {exc_value}")
        return True