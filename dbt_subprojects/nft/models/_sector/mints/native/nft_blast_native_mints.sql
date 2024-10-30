{{ config(
        
        schema = 'nft_blast',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='blast'
    , base_contracts = source('blast','contracts')
    , base_traces = source('blast','traces')
    , erc20_transfer = source('erc20_blast','evt_transfer')
    , base_transactions = source('blast','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
