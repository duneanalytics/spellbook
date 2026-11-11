{{
    config(
        schema='pyth_entropy_blast',
        alias='request',
        materialized='table',
        file_format='delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{{pyth_entropy_request(
    blockchain='blast',
    symbol='ETH',
    entropy_address='0x5744Cbf430D99456a0A8771208b674F27f8EF0Fb'
)}}