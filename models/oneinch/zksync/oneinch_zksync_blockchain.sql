{% set blockchain = 'zksync' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 324                                        as chain_id
    , 'ETH'                                      as native_token_symbol
    , 0x5aea5775959fbc2557cc8789bc1bf90a239d9a91 as wrapped_native_token_address
    , 'https://explorer.zksync.io'               as explorer_link
    , timestamp '2023-04-12 10:16'               as first_deploy_at