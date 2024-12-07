import os
from boto3 import client
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv(".env")


def s3_connection():
    """Connects to an S3"""
    s3 = client(
        "s3",
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    )
    return s3


def download_truck_data_files(s3, bucket: str) -> None:
    """Downloads relevant files from S3 to a data/ folder."""

    response = s3.list_objects_v2(
        Bucket=bucket, Prefix="bluesky/2024-12-07/", Delimiter='/')

    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']

            if key.endswith('.json') and key.count('/') == "bluesky/2024-12-07/".count('/'):
                file_obj = s3.get_object(Bucket=bucket, Key=key)
                file_content = file_obj['Body'].read().decode('utf-8')
                print(file_content)
    else:
        print("No files found in the parent folder.")


if __name__ == "__main__":
    s3 = s3_connection()
    bucket = "trendgineers-raw-firehose-data"

    download_truck_data_files(s3, bucket)
