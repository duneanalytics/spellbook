{{config(
    schema = 'tokens_ethereum',
    alias = 'transfers',
    materialized = 'view',
)
}}

SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'ethereum'