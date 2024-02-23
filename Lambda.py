import pandas as pd
import boto3
import os
from variables import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, S3_BUCKET_NAME, S3_OBJECT_NAME, FILE_PATH

def lambda_handler(event, context):
    # Set AWS credentials
    os.environ['AWS_ACCESS_KEY_ID'] = AWS_ACCESS_KEY_ID
    os.environ['AWS_SECRET_ACCESS_KEY'] = AWS_SECRET_ACCESS_KEY

    try:
        # Read data from the S3 bucket
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=S3_OBJECT_NAME)
        data = pd.read_csv(response['Body'])

        #calculate total bookings per destination
        bookings_per_destination = data.groupby('destination')['number_of_passengers'].sum().reset_index()

        # Store the data back to S3
        with open('/tmp/aggregated_data.csv', 'w') as f:
            bookings_per_destination.to_csv(f, index=False)

        s3.upload_file('/tmp/aggregated_data.csv', S3_BUCKET_NAME, 'aggregated_data.csv')

        return {
            'statusCode': 200,
            'body': 'Data aggregation completed and stored.'
        }
    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': 'Error occurred during data aggregation.'
        }
