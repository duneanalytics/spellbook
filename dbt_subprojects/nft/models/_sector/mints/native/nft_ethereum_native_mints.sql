{{ config(
        
        schema = 'nft_ethereum',
        alias = 'native_mints',
        partition_by = ['block_month'],
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='ethereum'
    , base_contracts = source('ethereum','contracts')
    , base_traces = source('ethereum','traces')
    , erc20_transfer = source('erc20_ethereum','evt_transfer')
    , base_transactions = source('ethereum','transactions')
    , eth_currency_contract = '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
)}}
