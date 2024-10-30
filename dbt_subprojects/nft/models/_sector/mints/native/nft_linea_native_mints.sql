{{ config(
        
        schema = 'nft_linea',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='linea'
    , base_contracts = source('linea','contracts')
    , base_traces = source('linea','traces')
    , erc20_transfer = source('erc20_linea','evt_transfer')
    , base_transactions = source('linea','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
