{{ config(
        schema = 'dex_bnb',
        alias='wash_trades',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index']
)
}}

{{dex_wash_trades(
    blockchain='bnb'
)}} 