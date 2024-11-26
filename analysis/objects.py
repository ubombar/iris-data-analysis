from dataclasses import dataclass
from datetime import datetime

@dataclass
class Measurement():
    '''This represents a measurement from describe tables.'''
    measurement_uuid: str 
    type: str 
    cleaned: bool 
    prefix: str 
    agent_uuid: str 
    table_name: str 
    retrieved: datetime
    processed: datetime