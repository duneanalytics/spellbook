{% set blockchain = 'mantle' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_mantle', 'net_transfers_daily_asset') }}
