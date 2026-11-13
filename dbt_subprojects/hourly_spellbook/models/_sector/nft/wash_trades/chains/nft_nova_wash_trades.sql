{{ config(

        schema = 'nft_nova',
        alias='wash_trades',
        partition_by=['block_month'],
        materialized='table',
        tags=['static'],
        file_format = 'delta',
)
}}

{{nft_wash_trades(
    blockchain='nova',
    first_funded_by= source('addresses_events_nova', 'first_funded_by')
)}}
