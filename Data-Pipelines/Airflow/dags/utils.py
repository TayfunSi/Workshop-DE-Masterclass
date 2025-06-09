# dags/utils.py
import pandas as pd
import os

def case1_process_taxi_zone():
    raw_path = "data/raw/TaxiZone.csv"
    processed_dir = "data/processed"
    processed_file = os.path.join(processed_dir, "TaxiZone_copied.csv")
    
    os.makedirs(processed_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    df.to_csv(processed_file, index=False)