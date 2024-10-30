{{ config(
        
        schema = 'nft_celo',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='celo'
    , base_contracts = source('celo','contracts')
    , base_traces = source('celo','traces')
    , erc20_transfer = source('erc20_celo','evt_transfer')
    , base_transactions = source('celo','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
