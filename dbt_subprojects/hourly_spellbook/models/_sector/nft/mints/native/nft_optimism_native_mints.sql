{{ config(
        
        schema = 'nft_optimism',
        alias = 'native_mints',
        partition_by = ['block_month'],
		file_format = 'delta',
        materialized = 'incremental',
		incremental_strategy = 'merge',
        incremental_predicates = ['DBT_INTERNAL_DEST.block_time >= date_trunc(\'day\', now() - interval \'7\' day)'],
        unique_key = ['tx_hash','evt_index','token_id','number_of_items']
        )
}}

{{nft_mints(
    blockchain='optimism'
    , base_contracts = source('optimism','contracts')
    , base_traces = source('optimism','traces')
    , erc20_transfer = source('erc20_optimism','evt_Transfer')
    , base_transactions = source('optimism','transactions')
    , eth_currency_contract = '0xdeaddeaddeaddeaddeaddeaddeaddeaddead0000'
)}}
