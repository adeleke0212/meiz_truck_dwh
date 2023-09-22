import psycopg2
import pandas as pd
from sqlalchemy import create_engine
from configparser import ConfigParser
from constants import db_tables
from sql.queries import Extract

config = ConfigParser()
config.read('.env')

HOST = config['DB_CONN']['host']
USER = config['DB_CONN']['user']
DB_DATABASE = config['DB_CONN']['database']
PORT = config['DB_CONN']['port']
PASSWORD = config['DB_CONN']['password']


conn = create_engine(
    f'postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB_DATABASE}'
)


# Approach 1, Load to local, exploration with pandas
for table in db_tables:
    df = pd.read_sql_query(Extract.query.format(table), con=conn)
    df.to_csv(f"Datasets/{table}.csv")
    print(f'Table {table} extracted')
