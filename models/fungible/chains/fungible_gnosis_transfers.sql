{{ config(
        
        schema = 'fungible_gnosis',
        alias='transfers',
)
}}

{{fungible_transfers(
    blockchain='gnosis'
    , native_symbol='xDAI'
    , traces = source('gnosis','traces')
    , transactions = source('gnosis','transactions')
    , erc20_transfers = source('erc20_gnosis','evt_Transfer')
    , erc20_tokens = source('tokens_gnosis', 'erc20')
)}}