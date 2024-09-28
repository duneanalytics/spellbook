{{ config(
        
        schema = 'nft_base',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='base'
    , base_contracts = source('base','contracts')
    , base_traces = source('base','traces')
    , erc20_transfer = source('erc20_base','evt_transfer')
    , base_transactions = source('base','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
