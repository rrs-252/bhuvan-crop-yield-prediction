import pandas as pd
from data.loaders.coordinate_loader import CoordinateLoader
from services.api_client import AgriAPIClient

class DataIntegrator:
    def __init__(self):
        self.coord_loader = CoordinateLoader(Config.COORDINATE_CSV)
        self.yield_loader = YieldDataLoader(Config.YIELD_CSV)
        self.api_client = AgriAPIClient()
        
    def process_and_save(self):
        yield_df = self.yield_loader.get_yield_data()
        records = []
        
        for _, row in yield_df.iterrows():
            district = row['Dist Name']
            year = row['Year']
            lat, lon = self.coord_loader.get_coordinates(district)
            
            if not lat or not lon:
                continue
                
            agri_data = self.api_client.get_agri_data(lat, lon, year)
            climate_data = self.api_client.get_climate_data(lat, lon, year)
            
            for crop in ['RICE', 'WHEAT', 'MAIZE']:
                yield_value = row.get(f'{crop} YIELD (Kg per ha)')
                if pd.notna(yield_value):
                    records.append({
                        'district': district,
                        'lat': lat,
                        'lon': lon,
                        'year': year,
                        'crop': crop.lower(),
                        **agri_data,
                        **climate_data,
                        'yield': yield_value
                    })
                    
        pd.DataFrame(records).to_csv(Config.OUTPUT_CSV, index=False)
        return Config.OUTPUT_CSV
