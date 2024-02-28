{{ config(
        
        schema = 'fungible_ethereum',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='ethereum'
    , native_symbol='ETH'
    , traces = source('ethereum','traces')
    , transactions = source('ethereum','transactions')
    , erc20_transfers = source('erc20_ethereum','evt_Transfer')
    , erc20_tokens = source('tokens_ethereum', 'erc20')
)}}