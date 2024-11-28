{{ config(
        
        schema = 'nft_zora',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='zora'
    , base_contracts = source('zora','contracts')
    , base_traces = source('zora','traces')
    , erc20_transfer = source('erc20_zora','evt_transfer')
    , base_transactions = source('zora','transactions')
    , eth_currency_contract = '0x4200000000000000000000000000000000000006'
)}}
