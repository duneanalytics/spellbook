{{ config(
        
        schema = 'nft_polygon',
        alias = 'native_mints',
        partition_by = ['block_month'],
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='polygon'
    , base_contracts = source('polygon','contracts')
    , base_traces = source('polygon','traces')
    , erc20_transfer = source('erc20_polygon','evt_transfer')
    , base_transactions = source('polygon','transactions')
    , eth_currency_contract = '0x0000000000000000000000000000000000001010'
)}}
