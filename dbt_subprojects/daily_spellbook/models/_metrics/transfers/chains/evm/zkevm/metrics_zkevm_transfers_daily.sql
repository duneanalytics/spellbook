{% set blockchain = 'zkevm' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ source('tokens_zkevm', 'net_transfers_daily') }}
