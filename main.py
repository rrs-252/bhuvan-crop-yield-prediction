import logging
from processors.data_integrator import AgriDataIntegrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

if __name__ == "__main__":
    integrator = AgriDataIntegrator()
    integrator.integrate()
    logging.info("Data integration completed successfully")
