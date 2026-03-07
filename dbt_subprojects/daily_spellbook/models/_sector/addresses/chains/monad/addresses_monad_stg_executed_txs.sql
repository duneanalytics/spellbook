{% set blockchain = 'monad' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_executed_txs',
		materialized = 'table',
		file_format = 'delta',
		partition_by = ['address_prefix'],
	)
}}

{{
	addresses_stg_executed_txs_agg(
		daily_model = ref('addresses_monad_stg_executed_txs_daily'),
	)
}}
