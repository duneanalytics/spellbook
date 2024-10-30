{{ config(
        
        schema = 'nft_sei',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='sei'
    , base_contracts = source('sei','contracts')
    , base_traces = source('sei','traces')
    , erc20_transfer = source('erc20_sei','evt_transfer')
    , base_transactions = source('sei','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
