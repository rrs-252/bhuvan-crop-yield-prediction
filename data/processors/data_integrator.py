import pandas as pd
import logging
from config.settings import Config
from services.vedas_client import VEDASClient

class AgriDataIntegrator:
    def __init__(self):
        self.vedas = VEDASClient()
        self.coord_df = self._load_coordinates()
        self.soil_df = self._load_soil_data()
        self.yield_df = self._load_yield_data()
        
    def _load_coordinates(self):
        df = pd.read_csv(Config.COORDINATE_CSV)
        return df[["District Name", "State", "Latitude", "Longitude"]]
    
    def _load_soil_data(self):
        soil_df = pd.read_csv(Config.SOIL_CSV)
        return soil_df.rename(columns={
            "OC - High": "organic_high",
            "OC - Medium": "organic_medium",
            "OC - Low": "organic_low",
            "pH - Acidic": "ph_acidic",
            "pH - Neutral": "ph_neutral",
            "pH - Alkaline": "ph_alkaline"
        })
    
    def _load_yield_data(self):
        return pd.read_csv(Config.YIELD_CSV)
    
    def _get_district_coords(self, district):
        district_data = self.coord_df[self.coord_df["District Name"] == district]
        if not district_data.empty:
            return (
                district_data["Latitude"].values[0],
                district_data["Longitude"].values[0],
                district_data["State"].values[0]
            )
        return None, None, None
    
    def _get_state_soil(self, state):
        state_data = self.soil_df[self.soil_df["State"] == state]
        if not state_data.empty:
            return state_data.iloc[0].to_dict()
        return None
    
    def integrate(self):
        records = []
        
        for _, yield_row in self.yield_df.iterrows():
            district = yield_row["Dist Name"]
            lat, lon, state = self._get_district_coords(district)
            
            if not lat or not lon:
                logging.warning(f"Skipping {district} - coordinates not found")
                continue
                
            soil_data = self._get_state_soil(state)
            if not soil_data:
                logging.warning(f"Skipping {district} - soil data not found for {state}")
                continue
                
            # Get VEDAS data
            ndvi_data = self.vedas.get_ndvi(lat, lon) or {}
            climate_data = self.vedas.get_climate(lat, lon) or {}
            
            record = {
                "district": district,
                "state": state,
                "year": yield_row["Year"],
                "lat": lat,
                "lon": lon,
                "ndvi": ndvi_data.get("ndvi"),
                "rainfall": climate_data.get("rainfall"),
                "temp_anomaly": climate_data.get("temp_anomaly"),
                **{k: soil_data[k] for k in [
                    "organic_high", "organic_medium", "organic_low",
                    "ph_acidic", "ph_neutral", "ph_alkaline"
                ]},
                "yield": yield_row["RICE YIELD (Kg per ha)"]
            }
            
            records.append(record)
            
        pd.DataFrame(records).to_csv(Config.OUTPUT_CSV, index=False)
        logging.info(f"Integrated data saved to {Config.OUTPUT_CSV}")
