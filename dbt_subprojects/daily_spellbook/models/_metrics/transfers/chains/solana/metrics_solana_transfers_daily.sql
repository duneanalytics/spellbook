{{ config(
        schema = 'metrics_solana'
        , alias = 'transfers_daily'
        , materialized = 'view'
       )
}}

SELECT *
FROM {{ source('tokens_solana', 'net_transfers_daily') }}