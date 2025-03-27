{{ config(
        schema = 'metrics_ton'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ ref('tokens_ton_net_transfers_daily') }}