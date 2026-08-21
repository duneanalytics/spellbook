{% set blockchain = 'zora' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_transfers',
		materialized = 'table',
		tags = ['static'],
		file_format = 'delta',
		partition_by = ['address_prefix'],
	)
}}

{{
	addresses_stg_transfers_agg(
		daily_model = ref('addresses_zora_stg_transfers_daily'),
	)
}}
