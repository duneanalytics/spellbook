{% set blockchain = 'blast' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'info',
		materialized = 'table',
		file_format = 'delta',
		partition_by = ['address_prefix'],
		tags = ['static'],
		post_hook = '{{ hide_spells() }}',
	)
}}

{{
	addresses_info_from_staging(
		blockchain = blockchain,
		staging_model = ref('addresses_blast_stg_info'),
	)
}}
