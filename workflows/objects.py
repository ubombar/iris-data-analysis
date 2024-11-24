from dataclasses import dataclass
from datetime import datetime

# This is the measurement data located in 'measurements'  folder.
@dataclass
class Measurement():
    measurement_uuid: str 
    type: str 
    cleaned: bool 
    prefix: str 
    agent_uuid: str 
    table_name: str 
    retrieved: datetime
    processed: datetime