{% set blockchain = 'zksync' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_zksync', 'net_transfers_daily_asset') }}
