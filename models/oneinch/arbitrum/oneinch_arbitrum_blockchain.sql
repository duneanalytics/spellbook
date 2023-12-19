{% set blockchain = 'arbitrum' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 42161                                      as chain_id
    , 'ETH'                                      as native_token_symbol
    , 0x82af49447d8a07e3bd95bd0d56f35241523fbab1 as wrapped_native_token_address
    , 'https://arbiscan.io'                      as explorer_link
    , timestamp '2021-06-22 10:27'               as first_deploy_at
