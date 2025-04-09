from data.processors.data_integrator import DataIntegrator

if __name__ == "__main__":
    integrator = DataIntegrator()
    output_path = integrator.process_and_save()
    print(f"Data processing complete. Saved to: {output_path}")
