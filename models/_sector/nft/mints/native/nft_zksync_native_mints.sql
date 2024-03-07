{{ config(
        
        schema = 'nft_zksync',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='zksync'
    , base_contracts = source('zksync','contracts')
    , base_traces = source('zksync','traces')
    , erc20_transfer = source('erc20_zksync','evt_transfer')
    , base_transactions = source('zksync','transactions')
    , eth_currency_contract = '0x000000000000000000000000000000000000800A'
)}}
