{% set blockchain = 'fantom' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 250                                        as chain_id
    , 'FTM'                                      as native_token_symbol
    , 0x21be370d5312f44cb42ce377bc9b8a0cef1a4c83 as wrapped_native_token_address
    , 'https://ftmscan.com'                      as explorer_link
    , timestamp '2022-03-16 16:20'               as first_deploy_at
    , 0xa218543cc21ee9388fa1e509f950fd127ca82155 as fusion_settlement_address