raw_data_schema = """
CREATE SCHEMA IF NOT EXIST raw_data_schema;
"""

banks = """
CREATE TABLE IF NOT EXISTS banks(
id VARCHAR primary key not null,
bank_name VARCHAR not null,
bank_code integer not null
);
"""

customers = """
CREATE TABLE IF NOT EXISTS customers(
id integer primary key not null,
customers_name VARCHAR not null,
customers_email not null,
customers_reg_date DATE not null,
customers_reg_year integer,
customers_reg_month integer,
customers_reg_day integer,
customers_reg_hour integer
);
"""
exchange_rates = """
CREATE TABLE IF NOT EXISTS exchange_rates(
exchange_rate,
bank_id VARCHAR,
date DATE
)
"""

items = """
CREATE TABLE IF NOT EXISTS items(
id integer primary key not null,
truck_name VARCHAR,
cost_price NUMERIC(7,2),
selling_price NUMERIC(7,2)
);
"""

transactions = """
CREATE TABLE IF NOT EXISTS transactions(
id integer primary key not null,
customer_id integer NOT NULL,
item_id integer not null,
date DATE not null,
bank_id VARCHAR not null,
qty integer not null,
transaction_year integer not null,
transaction_month integer not null,
transaction_day integer not null,
quarter integer not null

);
"""

dwh_raw_schema_tables = [banks, customers, exchange_rates, items, transactions]
