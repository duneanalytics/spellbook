{{ config(

        schema = 'nft_blast',
        alias='wash_trades',
        partition_by=['block_month'],
        materialized='table',
        file_format = 'delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
)
}}

{{nft_wash_trades(
    blockchain='blast',
    first_funded_by = source('addresses_events_blast', 'first_funded_by')
)}}
