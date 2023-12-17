{% set blockchain = 'avalanche_c' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 43114                                      as chain_id
    , 'AVAX'                                     as native_token_symbol
    , 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 as wrapped_native_token_address
    , 'https://snowtrace.io'                     as explorer_link
    , timestamp '2021-12-22 13:18'               as first_deploy_at
