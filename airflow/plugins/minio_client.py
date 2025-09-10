import json
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
from dotenv import load_dotenv

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET")
POKEMON_API_URL = os.getenv("POKEMON_API_URL")


def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(s3={"addressing_style": "path"})
    )

def ensure_bucket_exists(s3_client, bucket_name):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ["404", "NoSuchBucket"]:
            print(f"Bucket '{bucket_name}' does not exist. Creating it...")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"Bucket '{bucket_name}' created.")
        else:
            raise e

def store_to_minio(pokemon_data, pokemon_id):
    s3 = get_s3_client()
    ensure_bucket_exists(s3, MINIO_BUCKET)
    key = f"pokemon_{pokemon_id}.json"
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=json.dumps(pokemon_data),
        ContentType="application/json"
    )
    print(f"Stored Pokémon {pokemon_id} to MinIO as {key}")