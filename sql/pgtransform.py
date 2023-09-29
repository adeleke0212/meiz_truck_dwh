# Create table for fact and dims tables in dwh --- start_schema
pgstaging_schema = """
CREATE SCHEMA IF NOT EXISTS pgstaging_schema;
"""

# Staging schema table
dim_customers = """
CREATE TABLE IF NOT EXISTS pgstaging_schema.dim_customers(
id BIGINT IDENTITY(1, 1),
customers_name VARCHAR not null,
customers_email VARCHAR not null,
customers_reg_date DATE not null,
customers_reg_year integer,
customers_reg_month integer,
customers_reg_day integer,
customers_reg_hour integer
);
"""

dim_banks = """
CREATE TABLE IF NOT EXISTS pgstaging_schema.dim_banks(
id VARCHAR not null,
bank_name VARCHAR not null,
bank_code integer not null,
bank_exchange_rate NUMERIC,
exchange_rate_date DATE
);
"""
# exchange rate from exchange-rate table
dims_items = """
CREATE TABLE IF NOT EXISTS pgstaging_schema.dims_items(
id integer not null,
truck_name VARCHAR,
cost_price NUMERIC(7,2),
selling_price NUMERIC(7,2)
);
"""


ft_transactions = """
CREATE TABLE IF NOT EXISTS pgstaging_schema.ft_transactions(
id BIGINT IDENTITY(1, 1),
items_id integer not null,
truck_qty_sold integer not null,
truck_cost_price_usd NUMERIC(7,2),
truck_selling_price_naira NUMERIC(7,2),
total_trucks_cost_usd,
total_trucks_sold_naira,
transaction_date DATE,
transaction_year integer,
transaction_month integer,
transaction_day integer,
quarter integer,
customer_id integer NOT NULL,
bank_id VARCHAR not null
);
"""

transformed_pgschema_tables_query = [
    dim_customers, dim_banks, dims_items, ft_transactions]
