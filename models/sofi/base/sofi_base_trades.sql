{{ config(
    schema = 'sofi_base',
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
     (ref('friend_tech_base_base_trades'))
] %}

WITH trades AS (
    {{enrich_sofi_trades('base', base_trades)}}
    )

SELECT *
FROM trades
