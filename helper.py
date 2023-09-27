import boto3
from configparser import ConfigParser


config = ConfigParser()

config.read('.env')

BUCKET_NAME = config['AWS_S3']['bucket_name']
REGION = config['AWS_S3']['region']
ACCESS_KEY = config['AWS_S3']['access_key']
SECRET_KEY = config['AWS_S3']['secret_key']


def create_s3_bucket():
    client = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        region_name=REGION
    )
# Create client
    client.create_bucket(
        Bucket=BUCKET_NAME,
        CreateBucketConfiguration={
            'LocationConstraint': REGION
        }
    )


create_s3_bucket()
