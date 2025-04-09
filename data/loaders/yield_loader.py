import pandas as pd

class YieldDataLoader:
    def __init__(self, csv_path):
        self.df = pd.read_csv(csv_path)
        self._clean_data()
        
    def _clean_data(self):
        # Convert yield columns to numeric
        crops = ['RICE', 'WHEAT', 'MAIZE', 'PEARL MILLET','BARLEY','FINGER MILLETS']
        for crop in crops:
            self.df[f'{crop} YIELD (Kg per ha)'] = pd.to_numeric(
                self.df[f'{crop} YIELD (Kg per ha)'], errors='coerce'
            )
        self.df = self.df.dropna(subset=[f'{crop} YIELD (Kg per ha)' for crop in crops], how='all')
        
    def get_yield_data(self):
        return self.df
