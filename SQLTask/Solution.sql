--Solutions for the SQL Task
-- Link : http://sqlfiddle.com/#!17/5a0147/127


--Question 1 
WITH cumiltaive_sum_table as (
  SELECT 
    customer_id, 
    purchase_date,
    SUM(purchase_revenue) OVER(PARTITION BY customer_id ORDER BY purchase_date ASC) AS cumilitave_sum
  FROM test_orders
) 

SELECT 
  customer_id,
  min(purchase_date) as date

FROM cumiltaive_sum_table
WHERE cumilitave_sum > 100 
GROUP BY customer_id 


--Question 2 
SELECT 
	customer_id
FROM test_orders
GROUP BY customer_id
HAVING SUM(purchase_revenue) > 100


--Question 3
SELECT 
	customer_id,
	EXTRACT(DAY FROM max(purchase_date) -  min(purchase_date))  AS days_between_last_and_first_purchase, 
	SUM(purchase_revenue) /EXTRACT(DAY FROM max(purchase_date) -  min(purchase_date))  AS revenue_per_day

FROM test_orders
GROUP BY customer_id


--Question 4 
SELECT 
	test_orders.customer_id,
    test_order_details.order_id,
	CASE WHEN (number_items * purchase_price) -  purchase_revenue != 0 THEN (number_items * purchase_price) -  purchase_revenue ELSE NULL END AS difference ,
	CASE WHEN (number_items * purchase_price) -  purchase_revenue   = 0 THEN TRUE ELSE FALSE END AS verified
FROM test_orders
LEFT JOIN test_order_details 
	ON test_orders.customer_id = test_order_details.customer_id
	AND test_orders.purchase_date = test_order_details.purchase_timestamp



--Question 5 
SELECT 
	customer_id,
	SUM(CASE WHEN item_status = 'returned' THEN purchase_price ELSE 0 END)  AS extra_revenue,
	SUM(CASE WHEN item_status = 'returned' THEN - purchase_price 
		WHEN item_status = 'sold' THEN purchase_price 
		ELSE 0 END)  
		AS original_revenue,
	SUM (CASE WHEN item_status = 'sold' THEN purchase_price ELSE 0 END ) AS new_revenue,
	SUM (CASE WHEN item_status = 'sold' THEN purchase_price ELSE 0 END )
	/
	SUM(CASE WHEN item_status = 'returned' THEN - purchase_price 
		WHEN item_status = 'sold' THEN purchase_price 
		ELSE 0 END)  
	AS percentage_increase
	FROM test_order_details
	GROUP BY customer_id


--Question 6 
I would pay more attention to Customer 1 since he has the highest potential for increase if he doesnt return the items. This means we would be able to make 1.9 * profit he doesnt return the items he bough


--Question 7 
I would add an order ID in  the two tables that way we would not need to verify using the customer and date but using the ID. that would allow us to avoid problems in case the same customer has made more than one purchase per day