{{ config(
    schema = 'social_bnb',
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
    (ref('friend3_bnb_base_trades'))
] %}

WITH trades AS (
    {{enrich_social_trades('bnb', base_models, source('bnb', 'transactions'))}}
    )

SELECT *
FROM trades
