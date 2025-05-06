{{ config(

        schema = 'nft_nova',
        alias='wash_trades',
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['unique_trade_id']
)
}}

{{nft_wash_trades(
    blockchain='nova',
    first_funded_by= source('addresses_events_nova', 'first_funded_by')
)}}
