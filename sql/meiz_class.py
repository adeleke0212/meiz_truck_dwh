class_query = """

/* Q1. At what rate did Lemuel Solomon purchase the 2023 Ford Ranger? Return the Customer’s
Name, the name of the item, and the amount paid.
*/

SELECT 
	c.name AS customer_name
	, i.name AS item_name
	, ((i.selling_price * er.rate) * t.qty) AS total_amount_paid
FROM transactions t
LEFT JOIN customers AS c
ON t.customer_id = c.id
LEFT JOIN items i
ON t.item_id = i.id
LEFT JOIN exchange_rates er
ON t.bank_id = er.bank_id

WHERE c.name = 'Lemuel Solomon'
AND er.date = t.date
AND i.name LIKE '%Ranger%'

-- Note: There's no transaction recorded by Lemuel Solomon for item '2023 Ford Ranger'




/*
Q2. The Central Fx processor had a downtime that lasted several days. The company had to manually set exchange rates for sales that occurred during this period. 
During these hours, the rates retrieved from banks returned either 0 or NULL values.
Find out the customers who made purchases, the items they purchased, the total amount per
transaction in USD, and the code and names of the affected banks.
Ensure the customers who contributed the highest values are first on the list.
*/

SELECT 
	c.name AS customer_name
	, i.name AS item_name
	, (i.selling_price * t.qty) AS total_amount_USD
	, b.name AS bank_name
	, b.code AS bank_code
FROM transactions t
LEFT JOIN customers c
ON t.customer_id = c.id
LEFT JOIN items i
ON t.item_id = i.id
LEFT JOIN exchange_rates er
ON er.date = t.date
AND er.bank_id = t.bank_id
LEFT JOIN banks b
ON er.bank_id = b._id
WHERE rate = 0
OR rate IS NULL
ORDER BY total_amount_USD DESC




/*
Q3. Orji is a reseller. He placed an order of at least 12 trucks on 25th of October 2022 and made payments through a minimum of two banks. 
The trucks were not delivered as proposed by the company, hence, Orji requests a refund. You are to:
Find out when this order was placed. Based on your findings, attempt the next question.
deliver to the Audit department a list containing the following details:
    The name of the customer
    The date of purchase
    The name of each items purchased and their total values in USD
    The total amount paid by Orji for each of these transactions and the exchange rates applied for the transaction.
*/


-- First, check which customer named orji had at least 12 trucks purchased on 25th October 2022
SELECT 
	c.name AS customer_name
	, COUNT(DISTINCT t.bank_id) AS total_banks_used -- COUNT(DISTINCT column_name) helps you count the unique number of values present in that column
	, SUM(t.qty) AS total_trucks_purchased
FROM transactions t
LEFT JOIN customers c
ON t.customer_id = c.id
WHERE c.name LIKE '%Orji%'
AND t.date = '2022-10-25'
GROUP BY c.name

-- Seeing that both Orjis purchased more than 12 trucks on that day, we'll focus on both customers
SELECT 	
	c.name AS customer
	, t.date AS tx_date
	, i.name AS item_name
	, i.selling_price
	, (i.selling_price * t.qty) AS total_amount_USD
	, ((i.selling_price * er.rate) * t.qty) AS total_amount_paid
	, er.rate AS exchange_rate_applied
FROM transactions t
LEFT JOIN customers c
ON t.customer_id = c.id
LEFT JOIN banks b
ON t.bank_id = b._id
LEFT JOIN items i
ON t.item_id = i.id
LEFT JOIN exchange_rates er
ON t.bank_id = er.bank_id
AND er.date = t.date
WHERE c.name LIKE '%Orji%'
AND t.date = '2022-10-25'
ORDER BY c.name, total_amount_usd -- let all of one person's transactions appear before the other for easy read

solution.sql
Displaying solution.sql.

"""
