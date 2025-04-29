{{ config(
        schema = 'metrics_bitcoin'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_bitcoin', 'net_transfers_daily') }}