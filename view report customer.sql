/*
============================================================================
Customer Report
============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as names, ages and transaction details .
    2. Segment customers into categories (VIP, Regular, New) and age groups .
    3. Aggregates customer-level metrics :
       - total orders
       - total sales
       - total quantity purchased
       - total products
       - llifespan (in months)
    4. calulates valuable KPIs :
       - recency (months since last order)
       - average order value
       - average monthly spend
=============================================================================
*/

create view gold.report_customers as

with base_query as (
/* --------------------------------------------------------------------------
1) Base Query : Retrieves core columns from tables 
----------------------------------------------------------- */
select 
f.order_number,
f.product_key,
f.order_date,
f.sales_amount,
f.quantity,
c.customer_key,
c.customer_number,
CONCAT(c.first_name,' ',c.last_name) as customer_name,
datediff(year,c.birthdate,getdate()) age
from gold.fact_sales f
left join gold.dim_customers c
on c.customer_key=f.customer_key
where order_date is not null )

, customer_aggregation as ( 
/* --------------------------------------------------------------------------
2) Customer Aggregations : Summarize key matrics at the customer level
----------------------------------------------------------- */
select 
customer_key,
customer_number,
customer_name,
age,
count(DISTINCT order_number) as total_orders,
sum(sales_amount) as total_sales,
sum(quantity) as total_quantity,
count(DISTINCT product_key) as total_products,
max(order_date) as last_order_date,
datediff(month,min(order_date),max(order_date)) as lifespan
from base_query
group by 
    customer_key,
    customer_number,
    customer_name,
    age
)

select 
customer_key,
customer_number,
customer_name,
age,
case when age < 20 then 'Under 20'
     when age between 20 and 29 then '20-29'
     when age between 30 and 39 then '30-39'
     when age between 40 and 49 then '40-49'
     else '50 and above'
end as age_group,
case when total_sales > 5000 and lifespan >= 12 then 'VIP'
     when total_sales <= 5000 and lifespan >= 12 then 'Regular'
     else 'New'
end customer_segment,
last_order_date,
datediff(month,last_order_date,getdate()) as recency,
total_orders,
total_sales,
total_quantity,
total_products,
lifespan,
-- compute average order value (AVO) 
case when total_sales = 0 then 0
     else total_sales / total_orders
end as avg_order_value,
-- compute average monlthly spend
case when lifespan = 0 then total_sales
     else total_sales / lifespan
end as avg_monlthly_spend
from customer_aggregation