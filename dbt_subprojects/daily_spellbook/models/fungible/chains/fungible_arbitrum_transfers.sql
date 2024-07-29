{{ config(
        
        schema = 'fungible_arbitrum',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='arbitrum'
    , native_symbol='ETH'
    , traces = source('arbitrum','traces')
    , transactions = source('arbitrum','transactions')
    , erc20_transfers = source('erc20_arbitrum','evt_Transfer')
    , erc20_tokens = source('tokens_arbitrum', 'erc20')
)}}