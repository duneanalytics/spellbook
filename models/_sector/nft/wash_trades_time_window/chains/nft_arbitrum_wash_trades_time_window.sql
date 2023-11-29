{{ config(

        schema = 'nft_arbitrum',
        alias='wash_trades_time_window',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['unique_trade_id']
)
}}

{{nft_wash_trades_time_window(
    blockchain='arbitrum',
    first_funded_by= ref('addresses_events_arbitrum_first_funded_by')
)}}
