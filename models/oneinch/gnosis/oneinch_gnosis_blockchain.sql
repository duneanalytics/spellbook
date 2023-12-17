{% set blockchain = 'gnosis' %}



{{
    config(
        schema = 'oneinch_' + blockchain,
        alias = 'blockchain',
        materialized = 'table'
    )
}}



select 
    '{{ blockchain }}'                           as blockchain
    , 100                                        as chain_id
    , 'xDAI'                                     as native_token_symbol
    , 0xe91d153e0b41518a2ce8dd3d7944fa863463a97d as wrapped_native_token_address
    , 'https://gnosisscan.io'                    as explorer_link
    , timestamp '2021-12-22 13:21'               as first_deploy_at
