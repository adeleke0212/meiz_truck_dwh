# Insert into staging schema tables

dim_customers = """
INSERT INTO pgstaging_schema.dim_customers(
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
c.name as customers_name,
c.email as customers_email,
c.registered_at :: DATE as customers_reg_date,
extract( year from c.registered_at :: DATE) AS customers_reg_year,
extract( month from c.registered_at :: DATE) AS customers_reg_month,
extract( day from c.registered_at :: DATE) AS customers_reg_day,
SUBSTRING(c.registered_at :: VARCHAR from 12 for 2) :: integer as registration_hour
from raw_pgschema.customers c
"""

dim_banks = """
INSERT INTO pgstaging_schema.dim_banks(
id,
bank_name,
bank_code,
bank_exchange_rate,
exchange_rate_date
)
SELECT
b._id,
b.name as bank_name,
b.code as bank_code,
e.rate as exchange_rate,
e.date as exchange_rate_date
FROM raw_pgschema.banks b
JOIN raw_pgschema.exchange_rates e
ON
b._id = e.bank_id
"""

dim_items = """
INSERT INTO pgstaging_schema.dims_items(
id,
truck_name,
cost_price,
selling_price
)
SELECT
i.id,
i.name as truck_name,
i.cost_price,
i.selling_price
FROM raw_pgschema.items i
"""

ft_transactions = """
INSERT INTO pgstaging_schema.ft_transactions(
items_id,
truck_qty_sold,
truck_cost_price_usd,
truck_selling_price_naira,
total_trucks_cost_usd,
total_trucks_sold_naira,
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
i.cost_price as truck_cost_price_usd,
i.selling_price as truck_selling_price_naira,
(i.cost_price * t.qty) as total_trucks_cost_usd,
((i.selling_price * e.rate) * t.qty) as total_trucks_old_naira,
t.date :: DATE as transaction_date,
extract(year from t.date :: DATE) as transaction_year,
extract(month from t.date :: DATE) as transaction_month,
extract (day from t.date :: DATE) as transaction_day,
case
	when extract(month from t.date :: DATE) <= 3 THEN 1
	when extract(month from t.date :: DATE) > 3
		AND extract(month from t.date :: DATE) <= 6 THEN 2
	when extract(month from t.date :: DATE) > 6
		AND extract(month from t.date :: DATE) <= 9 THEN 3
	when extract(month from t.date :: DATE) > 9 THEN 4
end as quater,
t.customer_id,
t.bank_id
FROM raw_pgschema.transactions t
JOIN raw_pgschema.items i
ON 
i.id = t.item_id
JOIN raw_pgschema.exchange_rates e
ON
e.bank_id = t.bank_id

"""

pg_insert_queries = [dim_customers, dim_banks, ft_transactions]
