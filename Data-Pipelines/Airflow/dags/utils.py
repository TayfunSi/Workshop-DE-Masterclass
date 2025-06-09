# dags/utils.py
import pandas as pd
import os

def case1_copy_taxi_zone_manually():
    raw_path = "data/raw/TaxiZone.csv"
    processed_dir = "data/processed"
    processed_file = os.path.join(processed_dir, "TaxiZone_copied.csv")
    
    os.makedirs(processed_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    df.to_csv(processed_file, index=False)

def case2_copy_taxi_zone_on_file_entry():
    raw_path = "data/raw/TaxiZone_13062025.csv"
    processed_dir = "data/processed"
    processed_file = os.path.join(processed_dir, "TaxiZone_13062025.csv")
    
    os.makedirs(processed_dir, exist_ok=True)
    df = pd.read_csv(raw_path)
    df.to_csv(processed_file, index=False)

    # Datei in CLI kopieren und ablegen:
    # cp <zu kopierende Datei> <neuer Dateiname>