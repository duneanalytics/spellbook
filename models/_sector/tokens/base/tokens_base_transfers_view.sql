{{config(
    schema = 'tokens_base',
    alias = 'transfers',
    materialized = 'view',
)
}}

--note: had to name this file with `_view` suffix to avoid conflict with `transfers_base_transfers.sql` in `tokens` schema
SELECT
    *
FROM
    {{ ref('tokens_transfers') }}
WHERE
    blockchain = 'base'