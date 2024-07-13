{# This config is optional, as we've already set the default severity to warn #}
{{ config(severity='warn') }}

WITH order_details AS (
    SELECT
        order_id,
        COUNT(*) AS num_of_items_in_order

    FROM {{ ref('stg_ecommerce__order_items') }}
    GROUP BY 1
)

SELECT
    ord.order_id,
    ord.num_items_ordered,
    od.num_of_items_in_order

FROM {{ ref('stg_ecommerce__orders') }} AS ord
FULL OUTER JOIN order_details AS od USING(order_id)
WHERE
    -- All orders should have at least 1 item, and every item should tie to an order
    ord.order_id IS NULL
    OR od.order_id IS NULL
    -- Number of items doesn't match
    OR ord.num_items_ordered <> od.num_of_items_in_order