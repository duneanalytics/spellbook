{{ config(
        
        schema = 'fungible_bnb',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='bnb'
    , native_symbol='BNB'
    , traces = source('bnb','traces')
    , transactions = source('bnb','transactions')
    , erc20_transfers = source('erc20_bnb','evt_Transfer')
    , erc20_tokens = source('tokens_bnb', 'bep20')
)}}