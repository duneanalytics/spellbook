{% set blockchain = 'fantom' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_transfers_daily',
		materialized = 'table',
		tags = ['static'],
		file_format = 'delta',
		partition_by = ['block_month'],
	)
}}

{{
	addresses_stg_transfers_daily_union(
		received_model = ref('addresses_' + blockchain + '_stg_transfers_daily_received'),
		sent_model = ref('addresses_' + blockchain + '_stg_transfers_daily_sent'),
	)
}}
