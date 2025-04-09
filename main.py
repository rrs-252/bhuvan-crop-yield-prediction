from config import Config
from data.loaders import CoordinateLoader, YieldDataLoader
from data.processors import DataIntegrator
from services import AgriAPIClient

if __name__ == "__main__":
    # Initialize components
    coord_loader = CoordinateLoader(Config.COORDINATE_CSV)
    yield_loader = YieldDataLoader(Config.YIELD_CSV)
    integrator = DataIntegrator()
    
    # Process data and save to CSV
    output_path = integrator.process_and_save()
    print(f"Data processing complete. Saved to: {output_path}")
