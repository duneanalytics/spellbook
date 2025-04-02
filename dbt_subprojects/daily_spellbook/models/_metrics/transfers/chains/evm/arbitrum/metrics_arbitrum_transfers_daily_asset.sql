{% set blockchain = 'arbitrum' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_arbitrum', 'net_transfers_daily_asset') }}
