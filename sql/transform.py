# Create table for fact and dims tables in dwh --- start_schema
staging_schema = """
CREATE SCHEMA IF NOT EXISTS staging_schema;
"""

# Staging schema table
dim_customers = """
CREATE TABLE IF NOT EXISTS dim_customers(
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
CREATE TABLE IF NOT EXISTS dim_banks(
id VARCHAR not null,
bank_name VARCHAR not null,
bank_code integer not null,
bank_exchange_rate NUMERIC,
exchange_rate_date DATE
);
"""
# exchange rate from exchange-rate table
dims_items = """
CREATE TABLE IF NOT EXISTS dims_items(
id integer not null,
truck_name VARCHAR,
cost_price NUMERIC(7,2),
selling_price NUMERIC(7,2)
);
"""
# cost_price NUMERIC(7,2), from dims_items or items raw
# selling_price NUMERIC(7,2)
# item_id integer not null, from items
# total cost = qty * cost_price ---computed during insert
# total_sales_amount = selling price *qty ---computed during insert
# dates, year, month, day, quarter from transactions
# dims_customer id from dims customers table, ban_id from banks, items from items


ft_transactions = """
CREATE TABLE IF NOT EXISTS ft_transactions(
id BIGINT IDENTITY(1, 1),
items_id integer not null,
truck_qty_sold integer not null,
truck_cost_price_usd NUMERIC(7,2),
truck_selling_price_naira NUMERIC(7,2),
total_trucks_cost NUMERIC,
total_trucks_sales NUMERIC,
transaction_date DATE,
transaction_year integer,
transaction_month integer,
transaction_day integer,
quarter integer,
customer_id integer NOT NULL,
bank_id VARCHAR not null
);
"""

transformed_schema_tables_query = [
    dim_customers, dim_banks, dims_items, ft_transactions]
