{% set blockchain = 'bnb' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_info_executed_txs',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['address_prefix'],
		unique_key = ['address_prefix', 'address'],
	)
}}

{{
	addresses_info_executed_txs(
		transactions = source(blockchain, 'transactions'),
	)
}}
