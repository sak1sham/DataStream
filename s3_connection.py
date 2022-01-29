import boto3

bucket_name = 'cm-mongo-migration'

s3 = boto3.client("s3", region_name="us-east-1", aws_access_key_id=AWS_KEY_ID, aws_secret_access_key=AWS_SECRET)

file_name = "requirements.txt"
s3.upload_file(Filename=file_name, Bucket=bucket_name, Key=file_name)