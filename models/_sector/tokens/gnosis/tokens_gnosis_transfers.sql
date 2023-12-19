{{config(
    schema = 'tokens_gnosis',
    alias = 'transfers',
    materialized = 'view',
)
}}

SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'gnosis'