{{ config(
        schema = 'metrics_xrpl'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_xrpl', 'net_transfers_daily') }}