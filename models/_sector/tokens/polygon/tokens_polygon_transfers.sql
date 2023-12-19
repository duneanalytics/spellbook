{{config(
    schema = 'tokens_polygon',
    alias = 'transfers',
    materialized = 'view',
)
}}

SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'polygon'