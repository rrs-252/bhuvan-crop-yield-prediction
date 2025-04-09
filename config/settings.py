import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    BHUVAN_API_KEY = os.getenv("BHUVAN_API_KEY")
    VEDAS_API_KEY = os.getenv("VEDAS_API_KEY")
    
    # Path configurations
    COORDINATE_CSV = "data/raw/UnApportionedIdentifiers.csv"
    YIELD_CSV = "data/raw/ICRISAT-District-Level-Data.csv"
    OUTPUT_CSV = "data/processed/integrated_data.csv"
