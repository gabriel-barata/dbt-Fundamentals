{{
    config(
        materialized='table'
    )
}}

with customers as (

    SELECT *
    FROM {{ ref('stg_customers') }}

),

orders as (
    
    SELECT *
    FROM {{ ref('fct_orders') }}

),

customers_orders as (

    SELECT
        customer_id,
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        count(order_id) as number_of_orders,
        sum(amount) as lifetime_value
    FROM orders
    GROUP BY 1

),

final as (

    SELECT
        c.customer_id,
        c.first_name,
        c.last_name,
        co.first_order_date,
        co.last_order_date,
        coalesce(co.number_of_orders, 0) as number_of_orders,
        co.lifetime_value

    FROM customers c
    LEFT JOIN customers_orders co ON c.customer_id = co.customer_id

)

SELECT * FROM final