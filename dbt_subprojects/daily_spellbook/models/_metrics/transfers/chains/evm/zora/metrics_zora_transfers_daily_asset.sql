{% set blockchain = 'zora' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_zora', 'net_transfers_daily_asset') }}
