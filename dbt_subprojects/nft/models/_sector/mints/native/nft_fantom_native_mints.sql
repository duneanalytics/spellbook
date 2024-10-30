{{ config(
        
        schema = 'nft_fantom',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='fantom'
    , base_contracts = source('fantom','contracts')
    , base_traces = source('fantom','traces')
    , erc20_transfer = source('erc20_fantom','evt_transfer')
    , base_transactions = source('fantom','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
