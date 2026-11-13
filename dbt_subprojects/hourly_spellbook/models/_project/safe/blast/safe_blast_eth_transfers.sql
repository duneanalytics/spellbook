{{
    config(
        schema = 'safe_blast',
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        materialized='table',
        file_format ='delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'blast'
        , native_token_symbol = 'ETH'
        , project_start_date = '2024-02-24'
    )
}}
