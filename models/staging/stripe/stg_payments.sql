WITH payments AS (

    SELECT
        orderid as order_id,
        paymentmethod as payment_method,
        status,
        {{ cents_to_dollars ('amount')}} as amount,
        created,
        _batched_at
    FROM
        {{source('stripe', 'payment')}}
)

SELECT * FROM payments