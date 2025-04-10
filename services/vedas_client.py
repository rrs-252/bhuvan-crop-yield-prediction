import requests
import logging
from config.settings import Config

class VEDASClient:
    def __init__(self):
        self.base_url = "https://vedas.sac.gov.in/api"
        self.api_key = Config.VEDAS_API_KEY
        self.max_retries = 3

    def _make_request(self, endpoint, params):
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    f"{self.base_url}/{endpoint}",
                    params={**params, "key": self.api_key},
                    timeout=15
                )
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt+1} failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    logging.error(f"Final failure for {endpoint}")
                    return None
                
    def get_ndvi(self, lat, lon):
        return self._make_request("ndvi", {"lat": lat, "lon": lon})
    
    def get_climate(self, lat, lon):
        return self._make_request("climate", {"lat": lat, "lon": lon})

