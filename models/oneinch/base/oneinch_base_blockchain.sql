{% set blockchain = 'base' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 8453                                       as chain_id
    , 'ETH'                                      as native_token_symbol
    , 0x4200000000000000000000000000000000000006 as wrapped_native_token_address
    , 'https://basescan.org'                     as explorer_link
    , timestamp '2023-08-08 22:19'               as first_deploy_at
