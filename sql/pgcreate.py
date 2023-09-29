# Raw schema from extracted directly from pgadmin

raw_pgschema = """
CREATE SCHEMA IF NOT EXISTS raw_pgschema;
"""

banks = """
CREATE TABLE IF NOT EXISTS raw_pg_schema.banks(
_id VARCHAR primary key not null,
code integer not null,
name VARCHAR not null

);
"""


customers = """
CREATE TABLE IF NOT EXISTS raw_pg_schema.customers(
id integer primary key not null,
name VARCHAR not null,
email VARCHAR not null,
registered_at DATE

);
"""
exchange_rates = """
CREATE TABLE IF NOT EXISTS raw_pg_schema.exchange_rates(
date DATE,
bank_id VARCHAR,
rate NUMERIC(7,2)

)
"""

items = """
CREATE TABLE IF NOT EXISTS raw_pg_schema.items(
id integer primary key not null,
name VARCHAR,
selling_price NUMERIC(7,2)
cost_price NUMERIC(7,2)

);
"""

transactions = """
CREATE TABLE IF NOT EXISTS raw_pg_schema.transactions(
id integer primary key not null,
customer_id integer NOT NULL,
item_id integer not null,
date DATE not null,
bank_id VARCHAR,
qty integer not null

);
"""

dwh_pgraw_tables = [banks, customers, exchange_rates, items, transactions]
