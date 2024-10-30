{{ config(
        
        schema = 'nft_arbitrum',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='arbitrum'
    , base_contracts = source('arbitrum','contracts')
    , base_traces = source('arbitrum','traces')
    , erc20_transfer = source('erc20_arbitrum','evt_transfer')
    , base_transactions = source('arbitrum','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
