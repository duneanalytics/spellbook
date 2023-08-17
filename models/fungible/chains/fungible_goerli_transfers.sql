{{ config(
        tags = ['dunesql'],
        schema = 'fungible_goerli',
        alias=alias('transfers'),
        file_format = 'delta',
)
}}

{{nft_transfers(
    blockchain='goerli'
    , native_symbol='gETH'
    , traces = source('goerli','traces')
    , transactions = source('goerli','transactions')
    , erc20_transfers = source('erc20_goerli','evt_Transfer')
    , erc20_tokens = ref('tokens_goerli_erc20')
)}}