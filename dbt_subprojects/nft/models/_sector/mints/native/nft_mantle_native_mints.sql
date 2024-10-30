{{ config(
        
        schema = 'nft_mantle',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='mantle'
    , base_contracts = source('mantle','contracts')
    , base_traces = source('mantle','traces')
    , erc20_transfer = source('erc20_mantle','evt_transfer')
    , base_transactions = source('mantle','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
