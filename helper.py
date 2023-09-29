import boto3
from configparser import ConfigParser
import redshift_connector


config = ConfigParser()

config.read('.env')

BUCKET_NAME = config['AWS_S3']['bucket_name']
REGION = config['AWS_S3']['region']
ACCESS_KEY = config['AWS_S3']['access_key']
SECRET_KEY = config['AWS_S3']['secret_key']

# dwh-connection

DWH_HOST = config['DWH']['dwh_host']
DWH_ARN_ROLE = config['DWH']['arn_role']
DWH_USER = config['DWH']['dwh_username']
DWH_PASSWORD = config['DWH']['dwh_password']
DWH_DB = config['DWH']['dwh_database']


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


# create_s3_bucket()

# Create DWH connection


def connect_to_redshift():
    dwh_conn = redshift_connector.connect(
        host=DWH_HOST,
        password=DWH_PASSWORD,
        database=DWH_DB,
        user=DWH_USER
    )
    print('Connection to DWH esdtablished')
    return dwh_conn
