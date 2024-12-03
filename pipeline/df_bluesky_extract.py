import os
from boto3 import client
from dotenv import load_dotenv

load_dotenv(".env")

def s3_connection() -> client:
    """Connects to an S3 client."""
    s3 = client("s3", aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
                aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"))
    return s3

def download_truck_data_files(s3: client) -> None:
    """Downloads and prints the content of .txt files from S3."""
    continuation_token = None
    while True:
        # List objects with continuation token if there are more than 1000 files
        list_params = {'Bucket': os.environ.get("S3_BUCKET_NAME")}
        if continuation_token:
            list_params['ContinuationToken'] = continuation_token

        response = s3.list_objects_v2(**list_params)

        for file in response.get("Contents", []):  # Safely access Contents if it exists
            file_name = file['Key']

            if file_name.endswith(".txt"):
                print(f"Downloading file: {file_name}")
                # Get the object from S3
                file_obj = s3.get_object(Bucket=os.environ.get("S3_BUCKET_NAME"), Key=file_name)
                file_content = file_obj['Body'].read().decode('utf-8')  # Read and decode the content

                # Print the content of the file (or do something else with it)
                print(file_content)
                print("-" * 50)  # Separator between files

        # Check if there are more objects to retrieve
        continuation_token = response.get('NextContinuationToken')
        if not continuation_token:
            break
    return None

if __name__ == "__main__":
    s3 = s3_connection()
    download_truck_data_files(s3)
