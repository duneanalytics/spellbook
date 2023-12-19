{% set blockchain = 'polygon' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 137                                        as chain_id
    , 'MATIC'                                    as native_token_symbol
    , 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 as wrapped_native_token_address
    , 'https://polygonscan.com'                  as explorer_link
    , timestamp '2021-05-05 09:39'               as first_deploy_at
    , 0x1e8ae092651e7b14e4d0f93611267c5be19b8b9f as fusion_settlement_address