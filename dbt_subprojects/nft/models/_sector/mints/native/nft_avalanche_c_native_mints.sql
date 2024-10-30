{{ config(
        
        schema = 'nft_avalanche_c',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='avalanche_c'
    , base_contracts = source('avalanche_c','contracts')
    , base_traces = source('avalanche_c','traces')
    , erc20_transfer = source('erc20_avalanche_c','evt_transfer')
    , base_transactions = source('avalanche_c','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
