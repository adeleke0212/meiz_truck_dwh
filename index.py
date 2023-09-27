import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from constants import db_tables
from sql.queries import Extract
import os

config = ConfigParser()
config.read('.env')

HOST = config['DB_CONN']['host']
USER = config['DB_CONN']['user']
DB_DATABASE = config['DB_CONN']['database']
PORT = config['DB_CONN']['port']
PASSWORD = config['DB_CONN']['password']

# s3-conn
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

# Create connection engine via sqlalchemy
conn = create_engine(
    f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_DATABASE}'
)

s3_path = 's3://{}/{}.csv'

# Approach 1, Load to local, exploration and transformation with pandas
for table in db_tables:
    df = pd.read_sql_query(Extract.query.format(table), con=conn)
    df.to_csv(f"Datasets/{table}.csv")
    print(f'Table {table} extracted')

# Load to s3 - bucket
directory = 'transformed_Datasets'
for file in os.listdir(directory):
    df = pd.read_csv(f"transformed_Datasets/{file}")
    df.to_csv(
        s3_path.format(BUCKET_NAME, file), index=False,
        storage_options={'key': ACCESS_KEY, 'secret': SECRET_KEY}
    )
