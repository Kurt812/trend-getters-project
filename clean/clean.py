"""Cleans the database of old data"""

import logging
import os
from datetime import datetime, timezone, timedelta
import boto3
from boto3 import client
from dotenv import load_dotenv
from psycopg2.extensions import connection
from botocore.exceptions import NoCredentialsError, PartialCredentialsError


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

S3_CLIENT = boto3.client('s3')
DAY_LIMIT = 7
load_dotenv(".env")


def s3_connection() -> connection:
    """Connects to an S3"""
    try:
        aws_access_key_id = os.environ.get("AWS_ACCESS_KEY_ID")
        aws_secret_access_key = os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not aws_access_key_id or not aws_secret_access_key:
            logging.error("Missing required AWS credentials in .env file.")
            raise ValueError("Missing AWS credentials.")

        s3 = client("s3", aws_access_key_id,
                    aws_secret_access_key)
        return s3
    except (NoCredentialsError, PartialCredentialsError) as e:
        logging.error("A BotoCore error occurred: %s", e)
        raise
    except Exception as e:
        logging.error(
            "An unexpected error occurred while connecting to S3: %s", e)
        raise


def lambda_handler(event, context):
    """Lambda handler function to delete old files from S3"""
    try:
        s3_client = s3_connection()
        current_time = datetime.now(timezone.utc)

        response = s3_client.list_objects_v2(
            Bucket=os.environ.get("S3_BUCKET_NAME"), Prefix=os.environ.get("S3_OBJECT_PREFIX"))

        if 'Contents' not in response:
            logging.info("No objects found in the bucket.")
            return {"status": "No objects to clean"}

        deleted_files = []
        for obj in response['Contents']:
            object_key = obj['Key']
            last_modified = obj['LastModified']

            age = current_time - last_modified

            if age > timedelta(days=DAY_LIMIT):
                s3_client.delete_object(Bucket=os.environ.get(
                    "S3_BUCKET_NAME"), Key=object_key)
                deleted_files.append(object_key)
                logging.info("Deleted old object: %s", object_key)
            else:
                logging.info(
                    "Object is within retention period: %s", object_key)

        return {"status": "Completed", "deleted_files": deleted_files}

    except Exception as e:
        logging.error("Error during cleanup: %s", e)
        return {"status": "Failed", "error": str(e)}
