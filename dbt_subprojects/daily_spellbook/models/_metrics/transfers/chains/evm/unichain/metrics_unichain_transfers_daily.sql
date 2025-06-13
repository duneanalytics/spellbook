{% set blockchain = 'unichain' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_unichain', 'net_transfers_daily') }}