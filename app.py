


import os
import asyncio
import tempfile
from logbert_rca_pipeline_api import detect_anomalies_and_explain
import redis
import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import uvicorn


# FastAPI app
app = FastAPI()

# Initialize Redis client (adjust host/port/db as needed)
redis_client = redis.Redis(host='localhost', port=6379, db=0)

# Define the Redis queue name
REDIS_QUEUE = "log_queue"

# Request model
class LogRequest(BaseModel):
    filename: str


async def process_log(filename, file_content):
    # Save file_content to a temporary file and run RCA pipeline
    with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".log") as tmp:
        tmp.write(file_content)
        tmp_path = tmp.name
    loop = asyncio.get_event_loop()

    def _run_pipeline():
        return detect_anomalies_and_explain(tmp_path)
    results = await loop.run_in_executor(None, _run_pipeline)
    os.unlink(tmp_path)
    if results and len(results) > 0:
        return results[0]
    else:
        return {"filename": filename, "anomaly": False, "details": "No anomaly detected."}


S3_BUCKET = "your-s3-bucket-name"


async def get_file_from_s3(filename):
    loop = asyncio.get_event_loop()

    def _download():
        s3 = boto3.client("s3")
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=filename.decode(
            ) if isinstance(filename, bytes) else filename)
            return response["Body"].read()
        except ClientError as e:
            print(f"Error downloading {filename} from S3: {e}")
            return None
    return await loop.run_in_executor(None, _download)


async def main():
    while True:
        loop = asyncio.get_event_loop()
        filename = await loop.run_in_executor(None, redis_client.rpop, REDIS_QUEUE)
        if filename:
            file_content = await get_file_from_s3(filename)
            if file_content is not None:
                rca_result = await process_log(filename, file_content)
                await save_rca_to_db(rca_result)
                try:
                    await loop.run_in_executor(None, redis_client.lpush, READY_FOR_RCA_QUEUE, filename)
                    print(f"Notified {READY_FOR_RCA_QUEUE} for {filename}")
                except Exception as redis_exc:
                    print(f"Failed to notify ready-for-rca queue: {redis_exc}")
            else:
                print(f"File {filename} could not be downloaded from S3.")
        else:
            await asyncio.sleep(2)


# FastAPI endpoint to process a log file from S3
@app.post("/process-log")
async def process_log_endpoint(request: LogRequest):
    file_content = await get_file_from_s3(request.filename)
    if file_content is None:
        raise HTTPException(status_code=404, detail=f"File {request.filename} not found in S3 bucket.")
    result = await process_log(request.filename, file_content)
    return result

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
    else:
        asyncio.run(main())
