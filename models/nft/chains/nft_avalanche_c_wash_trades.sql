{{ config(
        tags = ['dunesql'],
        schema = 'nft_avalanche_c',
        alias=alias('wash_trades'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['unique_trade_id']
)
}}

{{nft_wash_trades(
    blockchain='avalanche_c',
    first_funded_by= ref('addresses_events_avalanche_c_first_funded_by')
)}}
