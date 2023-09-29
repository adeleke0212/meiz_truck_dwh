import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from constants import db_tables, pgdb_tables
from sql.queries import Extract
import os
from helper import connect_to_redshift
from sql.create import raw_data_schema, dwh_raw_schema_tables
from sql.pgcreate import dwh_pgraw_tables
from sql.pgtransform import transformed_pgschema_tables_query
from sql.transform import staging_schema, transformed_schema_tables_queries
from sql.pginsert import pg_insert_queries
from sql.insert import insert_queries


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


def load_to_local_pandasExplore():
    for table in db_tables:
        df = pd.read_sql_query(Extract.query.format(table), con=conn)
        df.to_csv(f"Datasets/{table}.csv")
        print(f'Table {table} extracted')

# load_to_local_pandasExplore()

# Load to s3 - bucket
# so realised in my bucket, i have .csv.csv extension so i performed the operation below
# .to_csv will normally add .csv


def loadPandasExploredDatasets_toS3():
    directory = 'transformed_Datasets'
    for file in os.listdir(directory):
        file_name = file.split('.')[0]
        df = pd.read_csv(f"transformed_Datasets/{file}")
        df.to_csv(
            s3_path.format(BUCKET_NAME, file_name), index=False,
            storage_options={'key': ACCESS_KEY, 'secret': SECRET_KEY}
        )


# loadPandasExploredDatasets_toS3()

# conn is defined globally so I can use in my function
# Option 2 extract directly from postgress to s3


def extractfromPostgresDb():
    for table in db_tables:
        query = f"select * from {table}"
        df = pd.read_sql_query(query, con=conn)
        df['registered_at'] = pd.to_datetime(df['registered_at'])
        df.to_csv(s3_path.format(BUCKET_NAME, f"pg{table}"), index=False, storage_options={
            'key': ACCESS_KEY, 'secret': SECRET_KEY
        })


extractfromPostgresDb()

# # # Create other tables in raw schema
raw_schema = 'raw_data_schema'

# # create the raw schema for copy tables in dwh and the


def create_raw_schema():
    dwh_conn = connect_to_redshift()
    cursor = dwh_conn.cursor()
    # Creating the raw schema
    cursor.execute(raw_data_schema)
    dwh_conn.commit()
    print('Raw schema created in dwh')
    cursor.close()
    dwh_conn.close()


# create_raw_schema()

# # Create raw schema dbtables


def create_pgraw_schema_tables():
    dwh_conn = connect_to_redshift()
    cursor = dwh_conn.cursor()
    for query in dwh_raw_schema_tables:
        print(f"==================={query[:55]}")
        cursor.execute(query)
        dwh_conn.commit()
    print('All tables created')
    cursor.close()
    dwh_conn.close()

# # Create tables for pandas explored Datasets


def create_raw_schema_tables():
    dwh_conn = connect_to_redshift()
    cursor = dwh_conn.cursor()
    for query in dwh_pgraw_tables:
        print(f"==================={query[:55]}")
        cursor.execute(query)
        dwh_conn.commit()
    print('All tables created')
    cursor.close()
    dwh_conn.close()

# # # Call the schema and tables function


create_pgraw_schema_tables()
create_raw_schema_tables()


# -- Copying pandas transformed tables from s3 to redshift, same schema
try:
    for table in db_tables:
        dwh_conn = connect_to_redshift()
        cursor = dwh_conn.cursor()
        print(f"Copying {table} from s3 to DWH")
        s3_copy_query = f"""
        copy {raw_schema}.{table}
        from '{s3_path.format(BUCKET_NAME, table)}'
        iam_role '{DWH_ROLE}'
        delimiter ','
        ignoreheader 1;
    """
        cursor.execute(s3_copy_query)
        dwh_conn.commit()
except Exception as e:
    print(e)

# Copying tables extracted from pgadmin from s3 to redshift

for pgtable in pgdb_tables:
    dwh_conn = connect_to_redshift()
    cursor = dwh_conn.cursor()
    print(f"Copying {pgtable} from s3 to DWH")
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

# ---Create Staging schema
connection = connect_to_redshift()
cursor = connection.cursor()
cursor.execute(staging_schema)
print('Staging schema created')
connection.commit()

# Create facts and dimension tables for my sql extracted files
for query in transformed_pgschema_tables_query:
    cursor.execute(query)
    connection.commit()
    print('facts and dims created for pg extracted data')


# Create fact and dimension tables from my pandas explored datasets
for _query in transformed_schema_tables_queries:
    cursor.execute(_query)
    connection.commit()
    print('facts and dims created for pandas explored data')

# Insert select from pg extracted tables
for query in pg_insert_queries:
    cursor.execute(query)
    connection.commit()
    print('Inserted into pg exctracted tables')

# Insert select for pandas explored tables
for query in insert_queries:
    cursor.execute(query)
    connection.commit()
    print('Inserted into pandas extracted tables')

# NOTE
# Since we only need 1 source of truth in the dwh, I removed the additional create stament for another schema
# We need just the only the staging schema and the copied data inside raw_schema as our source of truth here

# Additional data quality checks on the staging schema tables
staging_schema_tables = ['dim_customers', 'dim_banks', 'dims_items', 'ft_transactions',
                         'pgdim_customers', 'pgdim_banks', 'pgdims_items', 'pgft_transactions']

for table in staging_schema_tables():
    cursor.execute(f'select count(*) from staging_schema.{table}')
    print(f"Table {table} has {cursor.fetchall()} rows")
    connection.commit()

cursor.close()
connection.close()
