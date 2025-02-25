import os
import json
from bytewax.testing import run_main
from flow import build as build_flow

# Define the data source path for the input JSON files.
# Ensure the `data` folder exists and contains the correct files.

data_folder = "data"
data_source_path = []

# Check if the 'data' folder exists and contains JSON files
if os.path.exists(data_folder):
    data_source_path = [f"{data_folder}/{filename}" for filename in os.listdir(data_folder) if filename.endswith('.json')]
    
    if not data_source_path:
        raise FileNotFoundError(f"No JSON files found in the '{data_folder}' directory.")
else:
    raise FileNotFoundError(f"The '{data_folder}' directory does not exist.")

# Build the data flow using the specified data source path
flow = build_flow(in_memory=False, data_source_path=data_source_path)

if __name__ == "__main__":
    try:
        # Run the data ingestion process with Bytewax
        run_main(flow)
        print("Data ingestion complete!")
    except Exception as e:
        print(f"An error occurred during the ingestion process: {str(e)}")
