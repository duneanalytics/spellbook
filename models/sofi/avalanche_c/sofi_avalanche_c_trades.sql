{{ config(
    schema = 'sofi_avalanche_c',
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
     (ref('stars_arena_avalanche_c_base_trades'))
] %}

WITH trades AS (
    {{enrich_sofi_trades('avalanche_c', base_models, source('avalanche_c', 'transactions'))}}
    )

SELECT *
FROM trades
