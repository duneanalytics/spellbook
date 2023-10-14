{{ config(
    schema = 'sofi_arbitrum',
    tags = ['dunesql'],
    alias = alias('trades'),
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash','evt_index']
    )
}}

{% set base_models = [
    (ref('cipher_arbitrum_base_trades'))
    , (ref('post_tech_arbitrum_base_trades'))
] %}

WITH trades AS (
    {{enrich_sofi_trades('arbitrum', base_models, source('arbitrum', 'transactions'))}}
    )

SELECT *
FROM trades
