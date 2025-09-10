{% set blockchain = 'hyperevm' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transfers_daily_asset'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ ref('tokens_hyperevm_net_transfers_daily') }}


