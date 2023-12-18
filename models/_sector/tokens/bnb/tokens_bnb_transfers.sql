{{config(
    schema = 'tokens_bnb',
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
    blockchain = 'bnb'