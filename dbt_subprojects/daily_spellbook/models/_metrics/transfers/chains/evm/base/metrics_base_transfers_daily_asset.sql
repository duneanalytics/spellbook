{% set blockchain = 'base' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_base', 'net_transfers_daily_asset') }}
