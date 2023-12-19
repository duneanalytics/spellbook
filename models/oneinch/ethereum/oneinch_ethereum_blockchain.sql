{% set blockchain = 'ethereum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 1                                          as chain_id
    , 'ETH'                                      as native_token_symbol
    , 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2 as wrapped_native_token_address
    , 'https://etherscan.io'                     as explorer_link
    , timestamp '2019-06-03 20:11'               as first_deploy_at
