WITH orders as (

    SELECT 
        * 
    FROM
        {{ref('stg_orders')}}
),

payments as (

    SELECT 
        *
    FROM {{ref('stg_payments')}}
),

order_payments as (
    select
        order_id,
        sum(case when status = 'success' then amount end) as amount
    from payments
    group by 1
),

final as (

    SELECT
        orders.order_id,
        orders.customer_id,
        order_payments.amount
    FROM 
        orders
    LEFT JOIN 
        order_payments
          ON orders.order_id = order_payments.order_id
)

SELECT * FROM final