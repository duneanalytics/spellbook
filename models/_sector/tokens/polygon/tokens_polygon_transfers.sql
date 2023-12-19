{{config(
    schema = 'tokens_polygon',
    alias = 'transfers',
    materialized = 'view',
    tags = ['prod_exclude']
)
}}

SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'polygon'