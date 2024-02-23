import os
import boto3
from variables import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME, S3_OBJECT_NAME, FILE_PATH

def transfer_to_s3(file_path, bucket_name, object_name):
    # Create an S3 client
    s3 = boto3.client('s3')

    try:
        # Upload the file to S3
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"File uploaded to S3 bucket '{bucket_name}' with key '{object_name}'")
        return True
    except Exception as e:
        print(f"Error uploading file to S3: {e}")
        return False

# Set AWS credentials
os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY

# Upload the file to S3
transfer_to_s3(FILE_PATH, S3_BUCKET_NAME, S3_OBJECT_NAME)