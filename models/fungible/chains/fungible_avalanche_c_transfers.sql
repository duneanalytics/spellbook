{{ config(
        
        schema = 'fungible_avalanche_c',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='avalanche_c'
    , native_symbol='AVAX'
    , traces = source('avalanche_c','traces')
    , transactions = source('avalanche_c','transactions')
    , erc20_transfers = source('erc20_avalanche_c','evt_Transfer')
    , erc20_tokens = source('tokens_avalanche_c', 'erc20')
)}}