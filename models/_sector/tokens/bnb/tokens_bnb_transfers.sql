{{config(
    schema = 'tokens_bnb',
    alias = 'transfers',
    materialized = 'view',
)
}}

SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'bnb'