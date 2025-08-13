{{ config(
        schema = 'dex_bnb',
        alias='wash_trades',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['tx_hash', 'evt_index']
)
}}

{{dex_wash_trades(
    blockchain='bnb'
)}} 