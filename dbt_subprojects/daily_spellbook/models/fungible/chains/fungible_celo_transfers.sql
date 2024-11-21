{{ config(
        
        schema = 'fungible_celo',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='celo'
    , native_symbol='CELO'
    , traces = source('celo','traces')
    , transactions = source('celo','transactions')
    , erc20_transfers = source('erc20_celo', 'evt_transfer')
    , erc20_tokens = source('tokens_celo', 'erc20')
)}}
