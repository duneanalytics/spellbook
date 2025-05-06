{% set blockchain = 'linea' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_linea', 'net_transfers_daily_asset') }}
