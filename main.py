import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processors.data_integrator import AgriDataIntegrator
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    integrator = AgriDataIntegrator()
    integrator.integrate()
    logging.info("Data integration completed successfully")
