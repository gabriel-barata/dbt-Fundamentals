{{
    config(
        materialized='table'
    )
}}

with orders as (

    SELECT *
    FROM {{ ref('stg_orders') }}

),

payments as (

    SELECT *
    FROM {{ ref('stg_payments') }}

),

order_payments as (

    SELECT 
        order_id,
        SUM(CASE WHEN payment_status = 'success' THEN payment_amount END) as amount

    FROM payments
    GROUP BY 1

),

final as (
    
    SELECT
        o.order_id,
        o.customer_id,
        o.order_date,
        coalesce(p.amount, 0) as amount
    
    FROM orders o
    LEFT JOIN order_payments p ON o.order_id = p.order_id

)

SELECT * FROM final