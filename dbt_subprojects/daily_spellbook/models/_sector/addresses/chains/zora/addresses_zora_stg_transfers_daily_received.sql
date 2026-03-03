{% set blockchain = 'zora' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_transfers_daily_received',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['block_month'],
		unique_key = ['address_prefix', 'address', 'block_date'],
		incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
	)
}}

{{
	addresses_stg_transfers_daily_received(
		token_transfers = source('tokens_' + blockchain, 'transfers'),
	)
}}
