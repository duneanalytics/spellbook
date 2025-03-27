{{ config(
        schema = 'metrics_ton'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_ton', 'net_transfers_daily') }}