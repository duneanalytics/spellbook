{% set blockchain = 'bnb' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_executed_txs_daily',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['block_month'],
		unique_key = ['address_prefix', 'address', 'block_date'],
		incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
	)
}}

{{
	addresses_stg_executed_txs_daily(
		transactions = source(blockchain, 'transactions'),
	)
}}
