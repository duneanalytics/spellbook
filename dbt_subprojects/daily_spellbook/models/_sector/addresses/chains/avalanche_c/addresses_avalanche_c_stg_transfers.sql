{% set blockchain = 'avalanche_c' %}

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
		daily_model = ref('addresses_avalanche_c_stg_transfers_daily'),
	)
}}
