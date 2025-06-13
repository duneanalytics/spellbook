{% set blockchain = 'corn' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_corn', 'net_transfers_daily') }}