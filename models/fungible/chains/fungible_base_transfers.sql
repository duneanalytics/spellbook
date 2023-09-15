{{ config(
        tags = ['dunesql'],
        schema = 'fungible_base',
        alias=alias('transfers'),
)
}}

{{fungible_transfers(
    blockchain='base'
    , native_symbol='ETH'
    , traces = source('base','traces')
    , transactions = source('base','transactions')
    , erc20_transfers = source('erc20_base','evt_transfer')
    , erc20_tokens = ref('tokens_base_erc20')
)}}