import boto3
from aws.credentials import credentials
import pandas as pd

bucket_name = 'cm-mongo-migration'

s3 = boto3.client("s3", region_name="ap-south-1", aws_access_key_id=credentials['id'], aws_secret_access_key=credentials['key'])

file_name = "requirements2.txt"
s3.upload_file(Filename="temp/" + file_name, Bucket=bucket_name, Key=file_name)