{% set blockchain = 'ethereum' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_transfers',
		materialized = 'table',
		file_format = 'delta',
		partition_by = ['address_prefix'],
	)
}}

{{
	addresses_stg_transfers_agg(
		daily_model = ref('addresses_ethereum_stg_transfers_daily'),
	)
}}
