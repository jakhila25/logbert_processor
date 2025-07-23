import time
import os
import requests

UPLOAD_DIR = "/data/uploads"
PROCESSED_DIR = "/data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

def process_log(file_path):
    # Dummy anomaly detection logic; replace with your real pipeline
    # For example, call your detect_anomalies_and_explain here
    return {"filename": os.path.basename(file_path), "anomaly": True, "details": "Example anomaly detected."}

def save_rca_to_db(rca_result):
    # Replace with actual DB insert logic (e.g., via RCA API or direct DB connection)
    requests.post("http://rca_api:8000/rca/", json=rca_result)

def main():
    processed_files = set()
    while True:
        for filename in os.listdir(UPLOAD_DIR):
            file_path = os.path.join(UPLOAD_DIR, filename)
            if filename not in processed_files and os.path.isfile(file_path):
                rca_result = process_log(file_path)
                save_rca_to_db(rca_result)
                processed_files.add(filename)
        time.sleep(5)

if __name__ == "__main__":
    main()