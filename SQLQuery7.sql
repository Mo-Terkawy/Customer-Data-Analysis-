/* Change Over Time */
-- First way
select 
Year(order_date) as order_yaer,
month(order_date) as order_yaer,
sum(sales_amount) as total_sales,
count(DISTINCT customer_key) as total_customer,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by Year(order_date),month(order_date)
order by Year(order_date),month(order_date)

-- Second way
select 
DATETRUNC(month,order_date) as order_Date,
sum(sales_amount) as total_sales,
count(DISTINCT customer_key) as total_customer,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
order by DATETRUNC(month,order_date)

-- Third way
select 
format(order_date,'yyyy-MMM') as order_Date,
sum(sales_amount) as total_sales,
count(DISTINCT customer_key) as total_customer,
sum(quantity) as total_quantity
from gold.fact_sales
where order_date is not null
group by format(order_date,'yyyy-MMM')
order by format(order_date,'yyyy-MMM')

/* - Calculate the Total Sales per month
   - and the runinng total of sales over time */
select
order_date,
total_sales,
sum(total_sales) over(partition by order_date order by order_date) as running_total_sales
from
(
select 
DATETRUNC(month,order_date) as order_date,
sum(sales_amount) as total_sales
from gold.fact_sales
where order_date is not null
group by DATETRUNC(month,order_date)
)t

-- for year
select
order_date,
total_sales,
sum(total_sales) over(order by order_date) as running_total_sales,
AVG(avg_price) over(order by order_date) as moving_average_price
from
(
select 
DATETRUNC(year,order_date) as order_date,
sum(sales_amount) as total_sales,
AVG(price) as avg_price
from gold.fact_sales
where order_date is not null
group by DATETRUNC(year,order_date)
)t

/* Performance Analysis */
/* Analyze the yearly performance of products by comparing thier sales
   to both the average sales performance of the product and the perivous year's sales*/

with yearly_product_sales as (
select 
Year(f.order_date) as order_year,
p.product_name,
sum(sales_amount) as current_sales 
from gold.fact_sales f
left join gold.dim_products p
on f.product_key = p.product_key
where order_date is not null
GROUP by year(f.order_date),p.product_name
)
select 
order_year,
product_name,
current_sales,
avg(current_sales) over(partition by product_name),
current_sales - avg(current_sales) over(partition by product_name) as diff_avg,
case when current_sales - avg(current_sales) over(partition by product_name) > 0 then 'Above Avg'
     when current_sales - avg(current_sales) over(partition by product_name) < 0 then 'Below'
     else 'Avg'
End avg_change,
lag(current_sales) over(partition by product_name order by order_year) as py_sales,
current_sales - lag(current_sales) over(partition by product_name order by order_year) as diff_py,
case when current_sales - lag(current_sales) over(partition by product_name order by order_year) > 0 then 'Increase'
     when current_sales - lag(current_sales) over(partition by product_name order by order_year) < 0 then 'decrease'
     else 'No change'
End py_change
from yearly_product_sales
order by product_name, order_year

/* Part-to-Whole Analysis */
/* Which Categories contribute the most to Overall sales */
with category_sales as (
select 
category,
sum(sales_amount) as total_sales
from gold.fact_sales f
left join gold.dim_products p
on f.product_key=p.product_key
group by p.category
)
select 
category,
total_sales,
sum(total_sales) over() overall_sales,
concat(round((cast (total_sales as float) / sum(total_sales) over())* 100,2),'%') as percentage_of_sales_by_category
from category_sales
order by total_sales desc

/* Data Segmentation */
/* Segment products into cost ranges and
   count how many products fall into each segment */
with product_segment as (
select 
product_key,
product_name,
cost,
case when cost < 100 then 'Below 100'
     when cost between 100 and 500 then '100-500'
     when cost between 500 and 1000 then '500-1000'
     else 'Above 1000'
end cost_range
from gold.dim_products )

select 
cost_range,
count(product_key) as total_products
from product_segment
group by cost_range
order by total_products desc

/* Group customers into three segments based on thier spending behavior :
   - VIP : at least 12 months of history and spending more than $5000 .
   - Regular : at least 12 months of history but spending $5000 or less .
   - New : lifespan less than 12 months .
   And find the total number of customer by each group .*/
with customer_spending as (
select 
c.customer_key,
sum(f.sales_amount) as total_spending,
min(f.order_date) as first_order,
max(f.order_date) as last_order,
datediff(month,min(order_date),max(order_date)) as lifespan
from gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
group by c.customer_key 
)

select 
customer_segment,
count(customer_key) as total_customer
from (
    select 
    customer_key,
    total_spending,
    lifespan,
    case when total_spending > 5000 and lifespan >= 12 then 'VIP'
         when total_spending <= 5000 and lifespan >= 12 then 'Regular'
         else 'New'
    end customer_segment
    from customer_spending ) t
group by customer_segment
order by total_customer desc

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