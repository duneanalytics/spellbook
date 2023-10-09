{{ config(
        tags = ['dunesql'],
        schema = 'fungible_ethereum',
        alias=alias('transfers'),
        partition_by=['block_month'],
        materialized='incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['tx_hash', 'evt_index', 'from', 'to']
)
}}

{{fungible_transfers(
    blockchain='ethereum'
    , native_symbol='ETH'
    , traces = source('ethereum','traces')
    , transactions = source('ethereum','transactions')
    , erc20_transfers = source('erc20_ethereum','evt_Transfer')
    , erc20_tokens = ref('tokens_ethereum_erc20')
)}}