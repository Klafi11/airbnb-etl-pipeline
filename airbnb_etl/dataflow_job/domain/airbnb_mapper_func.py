from dataclasses import dataclass, fields 
import apache_beam as beam
import csv
from io import StringIO
import logging
from datetime import datetime
import os
import json
from datetime import date
import hashlib


NULL_VALUES = ("", None, "null", "Null", "NaN", "nan", "N/A", "n/a")

logger = logging.getLogger(__name__)

def dict_to_dataclass(data: dict, cls):
    """Convert a dict to a dataclass, handling null/empty values"""

    field_types = {f.name: f.type for f in fields(cls) if f.init}
    converted_dict = {}
    for key, value in data.items(): 
        if key not in field_types:  # Add this check
            continue
        try:
            typ = field_types[key]
            if value in NULL_VALUES:
                converted_dict[key] = None
            elif typ == date:
                converted_dict[key] = datetime.strptime(str(value), '%Y-%m-%d').date()
            elif typ == int:
                converted_dict[key] = int(float(str(value).replace(',', '')))
            elif typ == float:
                converted_dict[key] = float(str(value).replace(',', ''))
            else: 
                converted_dict[key] = typ(value)
        except (ValueError, TypeError) as e:
            logger.error(f"CONVERSION ERROR - Field: '{key}', Value: '{value}', Type: {typ}, Error: {e}")
            logger.error(f"FULL ROW DATA: {data}")
            raise
    record = cls(**converted_dict)
    #logger.debug(f"Successfully parsed record: {record}")
    return record

class ParseCsvToDataclass(beam.DoFn):
    def __init__(self, cls): 
        self.cls = cls
        self.fieldnames = [f.name for f in fields(cls) if f.init]
        self.expected_columns = len(self.fieldnames)
    
    def process(self, element): 

        raw_cols = next(csv.reader([element]))

        if len(raw_cols) != self.expected_columns:
            # Skip multiline csv 
            yield beam.pvalue.TaggedOutput("invalid", element)
            return

        reader = csv.DictReader(
                StringIO(element), 
                fieldnames=self.fieldnames,
                delimiter=",",
                quotechar='"',          
                doublequote=True,        
                skipinitialspace=True)
        
        row = next(reader)
        record = dict_to_dataclass(row, self.cls)
        record.processed_at = datetime.now()
        yield record


def load_schema():

    base_path =  os.path.dirname(__file__)
    
    schema_path = os.path.join(
        base_path,
        "beam",
        "schemas",
        "stg_airbnb_listings.json"
    )
    
    with open(schema_path, "r") as f:
        return json.load(f)


def generate_record_id(row_dict):
    """
    Hash the row contents to ensure idempotent writes.
    """
    # Convert dict to ordered string to avoid hash instability
    row_string = "|".join(f"{k}:{v}" for k, v in sorted(row_dict.items()))
    return hashlib.md5(row_string.encode("utf-8")).hexdigest()