{{ config(
        schema = 'metrics_xrpl'
        , alias = 'transfers_daily'
        , materialized = 'view'
        )
}}

SELECT *
FROM {{ ref('tokens_xrpl_net_transfers_daily') }}