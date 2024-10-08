import boto3
from io import StringIO

def load_to_s3(bucket_name, df, file_name, aws_access_key, aws_secret_key, aws_region):

    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)

    s3.put_object(
        Bucket=bucket_name,
        Key=f"{file_name}",
        Body=csv_buffer.getvalue()
    )
    print(f"File {file_name} successfully uploaded to {bucket_name}.")
