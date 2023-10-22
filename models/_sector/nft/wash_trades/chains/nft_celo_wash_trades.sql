{{
    config(
        tags = ['dunesql'],
        schema = 'nft_celo',
        alias = alias('wash_trades'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['unique_trade_id']
    )
}}

{{
    nft_wash_trades(
        blockchain = 'celo',
        first_funded_by = ref('addresses_events_celo_first_funded_by')
    )
}}
