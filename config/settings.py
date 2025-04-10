import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    VEDAS_API_KEY = os.getenv("VEDAS_API_KEY")
    COORDINATE_CSV = "data/raw/coordinates.csv"
    SOIL_CSV = "data/raw/soil_health.csv"
    YIELD_CSV = "data/raw/yield_data.csv"
    OUTPUT_CSV = "data/processed/integrated_data.csv"

