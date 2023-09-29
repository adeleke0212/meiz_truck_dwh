# Raw schema from extracted directly from pgadmin

pgraw_pgschema = """
CREATE SCHEMA IF NOT EXISTS raw_data_schema;
"""

pgbanks = """
CREATE TABLE IF NOT EXISTS raw_data_schema.banks(
_id VARCHAR primary key not null,
code integer not null,
name VARCHAR not null

);
"""


pgcustomers = """
CREATE TABLE IF NOT EXISTS raw_data_schema.customers(
id integer primary key not null,
name VARCHAR not null,
email VARCHAR not null,
registered_at DATE

);
"""
pgexchange_rates = """
CREATE TABLE IF NOT EXISTS raw_data_schema.exchange_rates(
date DATE,
bank_id VARCHAR,
rate NUMERIC(7,2)

)
"""

pgitems = """
CREATE TABLE IF NOT EXISTS raw_data_schema.items(
id integer primary key not null,
name VARCHAR,
selling_price NUMERIC(7,2)
cost_price NUMERIC(7,2)

);
"""

pgtransactions = """
CREATE TABLE IF NOT EXISTS raw_data_schema.transactions(
id integer primary key not null,
customer_id integer NOT NULL,
item_id integer not null,
date DATE not null,
bank_id VARCHAR,
qty integer not null

);
"""

dwh_pgraw_tables = [pgbanks, pgcustomers,
                    pgexchange_rates, pgitems, pgtransactions]
