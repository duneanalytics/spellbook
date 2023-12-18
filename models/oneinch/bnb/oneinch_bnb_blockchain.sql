{% set blockchain = 'bnb' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 56                                         as chain_id
    , 'BNB'                                      as native_token_symbol
    , 0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c as wrapped_native_token_address
    , 'https://bscscan.com'                      as explorer_link
    , timestamp '2021-02-18 14:37'               as first_deploy_at