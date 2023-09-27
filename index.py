import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from constants import db_tables, pgdb_tables
from sql.queries import Extract
import os
from helper import connect_to_redshift
from sql.create import raw_data_schema, dwh_raw_schema_tables

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

DWH_ROLE = config['DWH']['arn_role']
# Create connection engine via sqlalchemy
conn = create_engine(
    f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_DATABASE}'
)

s3_path = 's3://{}/{}.csv'

# Approach 1, Load to local, exploration and transformation with pandas
# read_sql not cursor.execute
# for table in db_tables:
#     df = pd.read_sql_query(Extract.query.format(table), con=conn)
#     df.to_csv(f"Datasets/{table}.csv")
#     print(f'Table {table} extracted')

# Load to s3 - bucket
# so realised in my bucket, i have .csv.csv extension so i performed the operation below
# .to_csv will normally add .csv
directory = 'transformed_Datasets'
for file in os.listdir(directory):
    file_name = file.split('.')[0]
    df = pd.read_csv(f"transformed_Datasets/{file}")
    df.to_csv(
        s3_path.format(BUCKET_NAME, file_name), index=False,
        storage_options={'key': ACCESS_KEY, 'secret': SECRET_KEY}
    )


# conn is defined globally so I can use in my function
# Option 2 extract directly from postgress to s3

def extractfromPostgresDb():
    for table in db_tables:
        query = f"select * from {table}"
        df = pd.read_sql_query(query, con=conn)
        df.to_csv(s3_path.format(BUCKET_NAME, f"pg{table}"), index=False, storage_options={
            'key': ACCESS_KEY, 'secret': SECRET_KEY
        })


# extractfromPostgresDb()

# # Create the create table command for raw schema in dwh


# def create_raw_schema():
#     dwh_conn = connect_to_redshift()
#     cursor = dwh_conn.cursor()
#     # Creating the dev schema
#     cursor.execute(raw_data_schema)
#     dwh_conn.commit()
#     print('Raw schema created in dwh')
#     cursor.close()
#     dwh_conn.close()


# # Create other tables in raw schema
raw_schema = 'raw_data_schema'


# def create_raw_schema_tables():
#     dwh_conn = connect_to_redshift()
#     cursor = dwh_conn.cursor()
#     for query in dwh_raw_schema_tables:
#         print(f"==================={query[:55]}")
#         cursor.execute(query)
#         dwh_conn.commit()
#     print('All tables created')
#     cursor.close()
#     dwh_conn.close()


# # # Call the schema and tables function

# create_raw_schema()
# create_raw_schema_tables()

# -- Copying pandas transformed from s3 to redshift
for table in db_tables:
    dwh_conn = connect_to_redshift()
    cursor = dwh_conn.cursor()
    s3_copy_query = f"""
    copy {raw_schema}.{table}
    from '{s3_path.format(BUCKET_NAME, table)}'
    iam_role '{DWH_ROLE}'
    delimiter ','
    ignoreheader 1;
"""
    cursor.execute(s3_copy_query)
    dwh_conn.commit()

# Copying tables extracted from pgadmin from s3 to redshift

for pgtable in pgdb_tables:
    dwh_conn = connect_to_redshift()
    cursor = dwh_conn.cursor()
    pgs3_copy_query = f"""
    copy {raw_schema}.{pgtable}
    from '{s3_path.format(BUCKET_NAME, pgtable)}'
    iam_role '{DWH_ROLE}'
    delimiter ','
    ignoreheader 1;
"""
    cursor.execute(pgs3_copy_query)
    dwh_conn.commit()

cursor.close()
dwh_conn.close()
