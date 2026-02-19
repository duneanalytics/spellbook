{% set blockchain = 'gnosis' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_info_transfers',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['address_prefix'],
		unique_key = ['address_prefix', 'address'],
	)
}}

{{
	addresses_info_transfers(
		token_transfers = source('tokens_' + blockchain, 'transfers'),
	)
}}
