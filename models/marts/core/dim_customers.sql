{{ config(
    materialized="table")
    }}


with customers as (
    SELECT 
        * 
    FROM
        {{ref('stg_customers')}}

),

orders as (

    SELECT 
        * 
    FROM
        {{ref('stg_orders')}}
),

payments as (

    SELECT
        *
    FROM
        {{ref('stg_payments')}}
),

customer_orders as (

    SELECT
        orders.customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS most_recent_order_date,
        COUNT(orders.order_id) AS number_of_orders,
        SUM(amount) as lifetime_value
    FROM 
        orders
    INNER JOIN
        payments
        ON orders.order_id = payments.order_id
    GROUP BY
        customer_id
),

final as (
    SELECT
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        COALESCE(customer_orders.number_of_orders, 0) as number_of_orders,
        lifetime_value
    FROM customers
    LEFT JOIN
        customer_orders using (customer_id)
)

SELECT * FROM final