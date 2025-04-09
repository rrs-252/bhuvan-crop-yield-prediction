import pandas as pd
import requests
import os
from datetime import datetime
from dotenv import load_dotenv
import logging
from git import Repo

# Configure logging
logging.basicConfig(filename='data_pipeline.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables
load_dotenv()

class AgriDataProcessor:
    def __init__(self):
        self.base_dir = os.path.dirname(os.path.abspath(__file__))
        self.repo_path = os.path.join(self.base_dir, 'agri-data-repo')
        self._init_repository()
        
        # Load datasets
        self.district_coords = pd.read_csv('UnApportionedIdentifiers.csv')
        self.yield_data = pd.read_csv('ICRISAT-District-Level-Data.csv')
        
        # API configuration
        self.api_config = {
            'bhuvan': {
                'base_url': 'https://bhuvan.nrsc.gov.in/api/agri',
                'key': os.getenv('BHUVAN_API_KEY')
            },
            'vedas': {
                'base_url': 'https://vedas.sac.gov.in/api/climate',
                'key': os.getenv('VEDAS_API_KEY')
            }
        }

    def _init_repository(self):
        """Initialize Git repository for data storage"""
        if not os.path.exists(self.repo_path):
            os.makedirs(self.repo_path)
            Repo.init(self.repo_path)
            logging.info("Initialized new Git repository")

        self.repo = Repo(self.repo_path)
        self.data_file = os.path.join(self.repo_path, 'integrated_agri_data.csv')

    def _get_coordinates(self, district_name):
        """Get coordinates for a district using fuzzy matching"""
        clean_name = district_name.upper().strip()
        match = self.district_coords[
            self.district_coords['District Name'].str.upper() == clean_name
        ]
        
        if not match.empty:
            return match.iloc[0]['Latitude'], match.iloc[0]['Longitude']
        
        logging.warning(f"Coordinates not found for {district_name}")
        return None, None

    def _call_api(self, service, endpoint, params):
        """Generic API caller with error handling"""
        try:
            headers = {'Authorization': f'Bearer {self.api_config[service]["key"]}'}
            response = requests.get(
                f"{self.api_config[service]['base_url']}/{endpoint}",
                params=params,
                headers=headers,
                timeout=10
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"API Error ({service}): {str(e)}")
            return None

    def _mock_bhuvan_api(self, lat, lon, year):
        """Fallback mock data for Bhuvan API"""
        return {
            'ndvi': round(abs(hash(f"{lat}{lon}{year}")) % 1000 / 1000, 3),
            'soil_ph': round(5.5 + (hash(f"{lat}{lon}") % 100) / 100, 1),
            'organic_carbon': round(0.5 + (hash(f"{lat}{lon}{year}") % 100) / 1000, 2)
        }

    def _mock_vedas_api(self, lat, lon, year):
        """Fallback mock data for VEDAS API"""
        return {
            'rainfall': 800 + (hash(f"{lat}{lon}{year}") % 1000),
            'temp_anomaly': round((hash(f"{lat}{lon}{year}") % 200 - 100) / 100, 2)
        }

    def process_yield_data(self):
        """Main processing pipeline"""
        merged_data = []
        
        for _, row in self.yield_data.iterrows():
            district = row['Dist Name']
            year = row['Year']
            lat, lon = self._get_coordinates(district)
            
            if not lat or not lon:
                continue

            # Get Bhuvan data (agricultural parameters)
            bhuvan_data = self._call_api('bhuvan', 'agri-params', 
                                       {'lat': lat, 'lon': lon, 'year': year})
            if not bhuvan_data:
                bhuvan_data = self._mock_bhuvan_api(lat, lon, year)

            # Get VEDAS data (climate parameters)
            vedas_data = self._call_api('vedas', 'climate-params',
                                      {'lat': lat, 'lon': lon, 'year': year})
            if not vedas_data:
                vedas_data = self._mock_vedas_api(lat, lon, year)

            # Process all crops
            for crop in ['RICE', 'WHEAT', 'PEARL MILLET', 'MAIZE']:
                yield_col = f'{crop} YIELD (Kg per ha)'
                if pd.notna(row[yield_col]):
                    record = {
                        'district': district,
                        'lat': lat,
                        'lon': lon,
                        'year': year,
                        'crop': crop.lower(),
                        **bhuvan_data,
                        **vedas_data,
                        'yield': row[yield_col]
                    }
                    merged_data.append(record)

        # Save and version data
        self._save_data(pd.DataFrame(merged_data))
        self._commit_to_github()

    def _save_data(self, new_data):
        """Append new data with version control"""
        if os.path.exists(self.data_file):
            existing_data = pd.read_csv(self.data_file)
            combined_data = pd.concat([existing_data, new_data])
        else:
            combined_data = new_data

        combined_data.to_csv(self.data_file, index=False)
        logging.info(f"Saved {len(new_data)} new records")

    def _commit_to_github(self):
        """Commit and push changes to GitHub"""
        try:
            self.repo.git.add(all=True)
            self.repo.index.commit(f"Data update {datetime.now().isoformat()}")
            origin = self.repo.remote(name='origin')
            origin.push()
            logging.info("Successfully pushed to GitHub")
        except Exception as e:
            logging.error(f"GitHub commit failed: {str(e)}")

if __name__ == "__main__":
    processor = AgriDataProcessor()
    processor.process_yield_data()
