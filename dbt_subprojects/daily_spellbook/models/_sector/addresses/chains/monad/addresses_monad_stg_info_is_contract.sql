{% set blockchain = 'monad' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_info_is_contract',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['address_prefix'],
		unique_key = ['address_prefix', 'address'],
	)
}}

{{
	addresses_info_is_contract(
		creation_traces = source(blockchain, 'creation_traces'),
		contracts = source(blockchain, 'contracts'),
	)
}}
