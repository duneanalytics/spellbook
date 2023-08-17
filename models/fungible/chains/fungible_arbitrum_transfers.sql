{{ config(
        tags = ['dunesql'],
        schema = 'fungible_arbitrum',
        alias=alias('transfers'),
        file_format = 'delta',
)
}}

{{fungible_transfers(
    blockchain='arbitrum'
    , native_symbol='ETH'
    , traces = source('arbitrum','traces')
    , transactions = source('arbitrum','transactions')
    , erc20_transfers = source('erc20_arbitrum','evt_Transfer')
    , erc20_tokens = ref('tokens_arbitrum_erc20')
)}}