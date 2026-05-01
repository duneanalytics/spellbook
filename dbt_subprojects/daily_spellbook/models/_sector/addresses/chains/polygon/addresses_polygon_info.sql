{% set blockchain = 'polygon' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'info',
		materialized = 'table',
		file_format = 'delta',
		partition_by = ['address_prefix'],
	)
}}

{{
	addresses_info_from_staging(
		blockchain = blockchain,
		staging_model = ref('addresses_polygon_stg_info'),
	)
}}
