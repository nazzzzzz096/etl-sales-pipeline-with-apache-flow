import boto3
import json
import datetime

s3 = boto3.client('s3')
source_bucket = 'sales-data-pipeline-buckett'
source_prefix = 'raw/'
target_prefix = 'processed/'

# List raw files
response = s3.list_objects_v2(Bucket=source_bucket, Prefix=source_prefix)

for obj in response.get('Contents', []):
    key = obj['Key']
    if key.endswith('.json'):
        raw = s3.get_object(Bucket=source_bucket, Key=key)
        content = raw['Body'].read().decode('utf-8')

        # Optional: add clean logic here
        # For now, just copy raw → processed
        processed_key = key.replace('raw/', 'processed/')
        s3.put_object(Bucket=source_bucket, Key=processed_key, Body=content)
        print(f"Copied {key} → {processed_key}")
