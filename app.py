
import time
import os
import redis

import asyncio
from sql import insert_rca_result, connect_to_database, disconnect_from_database

# Directories
UPLOAD_DIR = "/data/uploads"
PROCESSED_DIR = "/data/processed"
os.makedirs(PROCESSED_DIR, exist_ok=True)

# Redis configuration (must be set in environment variables)
REDIS_HOST = os.environ["REDIS_HOST"]
REDIS_PORT = int(os.environ["REDIS_PORT"])
REDIS_QUEUE = os.environ["REDIS_QUEUE"]
READY_FOR_RCA_QUEUE = os.environ.get("READY_FOR_RCA_QUEUE", "logbert_ready_for_rca")

# Initialize Redis client
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def process_log(file_path):
    # Dummy anomaly detection logic; replace with your real pipeline
    # For example, call your detect_anomalies_and_explain here
    return {"filename": os.path.basename(file_path), "anomaly": True, "details": "Example anomaly detected."}



def save_rca_to_db(rca_result):
    # Store anomaly result in rca_results table using sql.py
    async def _save():
        await connect_to_database()
        await insert_rca_result(rca_result)
        await disconnect_from_database()
    asyncio.run(_save())

def main():
    while True:
        # Block until a filename is available in the Redis queue
        filename = redis_client.rpop(REDIS_QUEUE)
        if filename:
            file_path = os.path.join(UPLOAD_DIR, filename)
            if os.path.isfile(file_path):
                rca_result = process_log(file_path)
                save_rca_to_db(rca_result)
                # Notify ready-for-rca queue
                try:
                    redis_client.lpush(READY_FOR_RCA_QUEUE, filename)
                    print(f"Notified {READY_FOR_RCA_QUEUE} for {filename}")
                except Exception as redis_exc:
                    print(f"Failed to notify ready-for-rca queue: {redis_exc}")
            else:
                print(f"File not found: {file_path}")
        else:
            time.sleep(2)

if __name__ == "__main__":
    main()