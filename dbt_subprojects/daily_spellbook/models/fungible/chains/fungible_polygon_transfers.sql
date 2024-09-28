{{ config(
        
        schema = 'fungible_polygon',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='polygon'
    , native_symbol='MATIC'
    , traces = source('polygon','traces')
    , transactions = source('polygon','transactions')
    , erc20_transfers = source('erc20_polygon','evt_Transfer')
    , erc20_tokens = source('tokens_polygon', 'erc20')
)}}