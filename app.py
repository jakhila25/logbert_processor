import os
import sys
import psycopg2
import json
from datetime import datetime

# Add the parent directory to sys.path so 'scripts' can be imported
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

import asyncio
import shutil
import tempfile
from logbert_rca_pipeline_api import detect_anomalies_and_explain
import redis
import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, UploadFile
from pydantic import BaseModel
import uvicorn

# FastAPI app
app = FastAPI()

# Initialize Redis client (adjust host/port/db as needed)
REDIS_HOST = os.getenv("REDIS_HOST", "redis-19932.c263.us-east-1-2.ec2.redns.redis-cloud.com")
REDIS_PORT = int(os.getenv("REDIS_PORT", 19932))  # ensure it's an int
REDIS_USERNAME = os.getenv("REDIS_USERNAME", "default")
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "SrPY3JUt6TXi33BPdDRDiW9fIplx2BJe")
REDIS_QUEUE = os.getenv("REDIS_QUEUE", "logbert_uploads")
S3_BUCKET = os.getenv("S3_BUCKET", "group13506")
S3_REGION = os.getenv("S3_REGION", "eu-north-1")
S3_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID", "AKIARVB2F2G73CRY5NS3")
S3_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "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")
DATABASE_URL = os.getenv("DATABASE_URL",
                         "postgresql://trans_owner:BookMyService7@ep-sweet-surf-a1qeduoy.ap-southeast-1.aws.neon.tech/logbert_rca?options=-csearch_path%3Dtrans")

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    username=REDIS_USERNAME,
    password=REDIS_PASSWORD,
    db=0,
    ssl=True  # Redis Cloud requires SSL
)

# Define the Redis queue name
# REDIS_QUEUE = "log_queue"
READY_FOR_RCA_QUEUE = True


# Request model
class LogRequest(BaseModel):
    filename: str


INPUT_LOG_PATH = os.path.join("AI_MODELS", "datasets", "Hadoop", "rca_abnormal_hadoop.log")


def save_uploaded_log(uploaded_file: UploadFile):
    """
    Save the uploaded log file to the RCA input path.
    """
    with open(INPUT_LOG_PATH, "wb") as f:
        shutil.copyfileobj(uploaded_file.file, f)
    uploaded_file.file.close()


async def process_log(filename, file_content):
    # Save file_content to a temporary file and run RCA pipeline
    # with tempfile.NamedTemporaryFile(delete=False, mode="wb", suffix=".log") as tmp:
    #     tmp.write(file_content)
    #     tmp_path = tmp.name
    os.makedirs(os.path.dirname(INPUT_LOG_PATH), exist_ok=True)
    with open(INPUT_LOG_PATH, "wb") as f:
        f.write(file_content)
    loop = asyncio.get_event_loop()

    def _run_pipeline():
        return detect_anomalies_and_explain(INPUT_LOG_PATH)

    results = await loop.run_in_executor(None, _run_pipeline)

    # os.unlink(tmp_path)
    if results and len(results) > 0:
        return results
    else:
        return {"filename": filename, "anomaly": False, "details": "No anomaly detected."}


async def get_file_from_s3(filename):
    loop = asyncio.get_event_loop()

    def _download():
        s3 = boto3.client(
            "s3",
            region_name=S3_REGION,
            aws_access_key_id=S3_ACCESS_KEY,
            aws_secret_access_key=S3_SECRET_KEY
        )
        try:
            response = s3.get_object(Bucket=S3_BUCKET, Key=filename.decode(
            ) if isinstance(filename, bytes) else filename)
            # print(f"response body = {response["Body"].read()}")
            return response["Body"].read()
        except ClientError as e:
            print(f"Error downloading {filename} from S3: {e}")
            return None

    return await loop.run_in_executor(None, _download)


async def save_rca_to_db(rca_result):
    pass


def insert_rca_result(filename, app_id, score, z_score, undetected_ratio, status, event_templates, explanation,
                      rootcause=None, ai_explanation=None):
    """
    Insert RCA result into PostgreSQL rca_results table.
    """
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        query = """
        INSERT INTO rca_results 
        (filename, app_id, score, z_score, undetected_ratio, status, events, explanation, logdate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, now())
        """

        cur.execute(query, (
            filename,
            app_id,
            float(score),
            float(z_score),
            float(undetected_ratio),
            status,
            json.dumps(event_templates),  # Convert Python dict/list to JSON string
            explanation
        ))
        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted RCA result for AppId: {app_id}")

    except Exception as e:
        print(f"Error inserting RCA result: {e}")


async def main():
    while True:
        loop = asyncio.get_event_loop()
        filename = await loop.run_in_executor(None, redis_client.rpop, REDIS_QUEUE)
        print(f"file name = {filename}")
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
    for record in result:
        insert_rca_result(
            filename="rca_abnormal_hadoop.log",
            app_id=record["AppId"],
            score=record["Score"],
            z_score=record["z_score"],
            undetected_ratio=record["UndetectedRatio"],
            status=record["status"],
            event_templates=record["Events"],
            explanation=record["Explanation"]
        )

    return result


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "serve":
        uvicorn.run("app:app", host="0.0.0.0", port=8000, reload=True)
    else:
        asyncio.run(main())
