{% set blockchain = 'hyperevm' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_hyperevm', 'net_transfers_daily') }}


