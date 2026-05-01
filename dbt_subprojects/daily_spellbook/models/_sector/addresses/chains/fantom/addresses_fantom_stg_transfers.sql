{% set blockchain = 'fantom' %}

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
		daily_model = ref('addresses_fantom_stg_transfers_daily'),
	)
}}
