{{ config(
        
        schema = 'nft_scroll',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='scroll'
    , base_contracts = source('scroll','contracts')
    , base_traces = source('scroll','traces')
    , erc20_transfer = source('erc20_scroll','evt_transfer')
    , base_transactions = source('scroll','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
