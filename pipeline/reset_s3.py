"""Deletes all objects from S3 Bucket"""

import os
import logging
import boto3
from dotenv import load_dotenv

load_dotenv(".env")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
    ]
)

def delete_all_objects_from_s3(bucket_name: str, prefix: str) -> None:
    """Deletes all objects in an S3 bucket."""
    s3_client = boto3.client('s3')

    try:
        continuation_token = None
        buckert_parameters = {
                'Bucket': bucket_name,
                'Prefix': prefix
            }
        while True:

            if continuation_token:
                buckert_parameters['ContinuationToken'] = continuation_token

            objects = s3_client.list_objects_v2(**buckert_parameters)

            if 'Contents' in objects:
                for obj in objects['Contents']:
                    logging.info("Deleting object: %s", obj['Key'])
                    s3_client.delete_object(Bucket=bucket_name, Key=obj['Key'])

                logging.info("Successfully deleted %s objects.", len(objects['Contents']))

            if objects.get('IsTruncated'):
                continuation_token = objects.get('NextContinuationToken')
            else:
                # No more objects to delete
                break

        logging.info("All objects under the prefix have been deleted.")
    except FileNotFoundError as e:
        logging.info("Error: %s",e)

if __name__ == "__main__":
    delete_all_objects_from_s3(os.environ.get("S3_BUCKET_NAME"),
                               os.environ.get("S3_OBJECT_PREFIX"))
