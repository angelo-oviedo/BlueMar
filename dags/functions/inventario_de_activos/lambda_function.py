import csv
import re
import pandas as pd
import boto3
from io import StringIO

s3 = boto3.client('s3')

def get_latest_file(bucket_name, prefix):
    """
    Retrieve the latest file in the specified S3 folder (prefix).
    """
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = response.get('Contents', [])
    
    csv_files = [file['Key'] for file in files if file['Key'].endswith('.csv')]
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in s3://{bucket_name}/{prefix}")
    
    latest_file = max(csv_files, key=lambda x: x.split('/')[-1])  
    return latest_file

def clean_and_prepare_airbyte_file(input_bucket, input_key, output_bucket, output_key):
    """
    Cleans the Airbyte CSV file from S3 and uploads the cleaned file back to S3.
    - Removes the first four metadata columns.
    - Standardizes headers.
    - Sanitizes numeric fields and retains nulls for Snowflake ingestion.
    """
    response = s3.get_object(Bucket=input_bucket, Key=input_key)
    raw_csv = response['Body'].read().decode('utf-8')

    csv_reader = csv.reader(StringIO(raw_csv))
    raw_headers = next(csv_reader)

    headers = [
        header.strip().replace(" ", "_").lower()
        for header in raw_headers[4:]
    ]

    cleaned_data = []
    for row in csv_reader:
        cleaned_row = {header: "" for header in headers}
        for idx, header in enumerate(headers):
            if idx + 4 < len(row):  
                value = row[idx + 4].strip()
                if "costo" in header or "total" in header or "cantidad" in header:
                    value = re.sub(r'[₡,]', '', value)
                    value = value if value.replace('.', '', 1).isdigit() else None
                cleaned_row[header] = value if value != "" else None
        cleaned_data.append(cleaned_row)

    df = pd.DataFrame(cleaned_data)

    output_csv = StringIO()
    df.to_csv(output_csv, index=False, encoding='utf-8')
    output_csv.seek(0)

    s3.put_object(Bucket=output_bucket, Key=output_key, Body=output_csv.getvalue())
    print(f"Cleaned file uploaded to s3://{output_bucket}/{output_key}")

def lambda_handler(event, context):
    """
    AWS Lambda handler function.
    """
    input_bucket = event['input_bucket']
    output_bucket = event['output_bucket']
    output_key = event['output_key']

    folder_prefix = "raw-data/Inventario_de_activos/"
    input_key = get_latest_file(input_bucket, folder_prefix)

    print(f"Processing file: s3://{input_bucket}/{input_key}")

    clean_and_prepare_airbyte_file(input_bucket, input_key, output_bucket, output_key)

    return {
        'statusCode': 200,
        'body': f"File processed and uploaded to s3://{output_bucket}/{output_key}"
    }