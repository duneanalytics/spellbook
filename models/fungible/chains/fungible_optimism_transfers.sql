{{ config(
        
        schema = 'fungible_optimism',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='optimism'
    , native_symbol='ETH'
    , traces = source('optimism','traces')
    , transactions = source('optimism','transactions')
    , erc20_transfers = source('erc20_optimism','evt_Transfer')
    , erc20_tokens = ref('tokens_optimism_erc20')
)}}