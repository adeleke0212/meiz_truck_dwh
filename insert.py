# Insert into staging schema tables

dim_customers = """
INSERT INTO staging_schema.dim_customers(
id,
customers_name,
customers_email,
customers_reg_date,
customers_reg_year,
customers_reg_month,
customers_reg_day,
customers_reg_hour
)
SELECT
c.id,
c.customers_name,
c.customers_email,
c.customers_reg_date,
c.customers_reg_year,
c.customers_reg_month,
c.ustomers_reg_day,
c.customers_reg_hour
from raw_data_schema.customers c

"""

dim_banks = """
INSERT INTO staging_schema.dim_banks(
id,
bank_name,
bank_code,
bank_exchange_rate,
exchange_rate_date
)
SELECT
b.id,
b.bank_name,
b.bank_code,
e.exchange_rate,
e.date as exchange_rate_date
FROM raw_data_schema.banks b
JOIN exchange_rates e
ON
b.id = e.bank_id
"""

dim_items = """
INSERT INTO staging_schema.dims_items(
id,
truck_name,
cost_price,
selling_price
)
SELECT
i.id,
i.truck_name,
i.cost_price,
i.selling_price
FROM raw_data_schema.items i
"""

ft_transactions = """
INSERT INTO staging_schema.ft_transactions(
items_id,
truck_qty_sold,
truck_cost_price_usd,
truck_selling_price_naira,
total_trucks_cost,
total_trucks_sales,
transaction_date,
transaction_year,
transaction_month,
transaction_day,
quarter,
customer_id,
bank_id
)
SELECT
t.item_id,
t.qty as truck_qty_sold,
i.cost_price as truck_cost_price,
i.selling_price as truck_selling_price,
(i.cost_price * t.qty) as total_trucks_cost,
((i.selling_price * e.rate) * t.qty) as total_trucks_sales_naira,
t.date as transaction_date,
t.transaction_year,
t.transaction_month,
t.transaction_day,
t.quarter,
t.customer_id,
t.bank_id
FROM raw_data_schema.transactions t
JOIN raw_data_schema.items i
ON 
i.id = t.item_id
JOIN raw_data_schema.exchange_rates e
ON
e.bank_id = t.bank_id
"""

insert_query = [dim_customers, dim_banks, ft_transactions]
