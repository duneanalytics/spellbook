{{config(
    schema = 'tokens_avalanche_c',
    alias = 'transfers',
    materialized = 'view',
)
}}

SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'avalanche_c'