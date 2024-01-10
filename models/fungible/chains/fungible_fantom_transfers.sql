{{ config(
        
        schema = 'fungible_fantom',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='fantom'
    , native_symbol='FTM'
    , traces = source('fantom','traces')
    , transactions = source('fantom','transactions')
    , erc20_transfers = source('erc20_fantom','evt_Transfer')
    , erc20_tokens = source('tokens_fantom', 'erc20')
)}}