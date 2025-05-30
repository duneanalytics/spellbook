{% set blockchain = 'optimism' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_optimism', 'net_transfers_daily_asset') }}
