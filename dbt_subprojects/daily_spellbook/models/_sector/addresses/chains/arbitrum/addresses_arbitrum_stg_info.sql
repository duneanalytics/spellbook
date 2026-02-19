{% set blockchain = 'arbitrum' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_info',
		materialized = 'incremental',
		file_format = 'delta',
		incremental_strategy = 'merge',
		partition_by = ['address_prefix'],
		unique_key = ['address_prefix', 'address'],
	)
}}

{{
	addresses_info_join(
		blockchain = blockchain,
		executed_txs_model = ref('addresses_arbitrum_stg_info_executed_txs'),
		transfers_model = ref('addresses_arbitrum_stg_info_transfers'),
		is_contract_model = ref('addresses_arbitrum_stg_info_is_contract'),
	)
}}
