{% set blockchain = 'optimism' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select
    '{{ blockchain }}'                           as blockchain
    , 10                                         as chain_id
    , 'ETH'                                      as native_token_symbol
    , 0x4200000000000000000000000000000000000006 as wrapped_native_token_address
    , 'https://explorer.optimism.io'             as explorer_link
    , timestamp '2021-11-12 09:07'               as first_deploy_at
