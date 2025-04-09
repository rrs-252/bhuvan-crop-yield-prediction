import pandas as pd
from fuzzywuzzy import fuzz

class CoordinateLoader:
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)
        self._clean_data()
        
    def _clean_data(self):
        self.df['clean_district'] = self.df['District Name'].str.upper().str.strip()
        self.coord_map = self.df.set_index('clean_district')[['Latitude', 'Longitude']].to_dict('index')
        
    def get_coordinates(self, district_name):
        clean_name = district_name.upper().strip()
        if clean_name in self.coord_map:
            return self.coord_map[clean_name]['Latitude'], self.coord_map[clean_name]['Longitude']
        
        # Fuzzy matching
        best_match = max(
            [(district, fuzz.token_set_ratio(clean_name, district)) 
             for district in self.coord_map.keys()],
            key=lambda x: x[1]
        )
        return self.coord_map[best_match[0]].values() if best_match[1] > 80 else (None, None)
