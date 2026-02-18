{% set blockchain = 'linea' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'info',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['address_prefix'],
		unique_key = ['address_prefix', 'address'],
	)
}}

{{
	addresses_info_incremental(
		blockchain = blockchain,
		staging_model = ref('addresses_linea_stg_info'),
	)
}}
