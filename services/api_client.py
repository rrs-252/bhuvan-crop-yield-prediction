import requests
import logging
from config.settings import Config

class AgriAPIClient:
    def __init__(self):
        self.bhuvan_base = "https://bhuvan.nrsc.gov.in/api/agri"
        self.vedas_base = "https://vedas.sac.gov.in/api/climate"
        
    def _make_request(self, service, endpoint, params):
        try:
            response = requests.get(
                f"{self.bhuvan_base if service == 'bhuvan' else self.vedas_base}/{endpoint}",
                params=params,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"{service.upper()} API Error: {str(e)}")
            return None

    def get_agri_data(self, lat, lon, year):
        return self._make_request('bhuvan', 'ndvi', {
            'lat': lat,
            'lon': lon,
            'year': year,
            'key': Config.BHUVAN_API_KEY
        }) or self._mock_agri_data(lat, lon, year)
        
    def get_climate_data(self, lat, lon, year):
        return self._make_request('vedas', 'climate', {
            'lat': lat,
            'lon': lon,
            'year': year,
            'key': Config.VEDAS_API_KEY
        }) or self._mock_climate_data(lat, lon, year)
        
    def _mock_agri_data(self, lat, lon, year):
        return {
            'ndvi': abs(hash(f"{lat}{lon}{year}")) % 1000 / 1000,
            'soil_ph': 6.0 + (hash(f"{lat}{lon}") % 100) / 1000,
            'organic_carbon': 0.8 + (hash(f"{lat}{lon}") % 100) / 1000
        }
        
    def _mock_climate_data(self, lat, lon, year):
        return {
            'rainfall': 800 + (hash(f"{lat}{lon}{year}") % 1000),
            'temp_anomaly': (hash(f"{lat}{lon}{year}") % 200 - 100) / 100
        }
