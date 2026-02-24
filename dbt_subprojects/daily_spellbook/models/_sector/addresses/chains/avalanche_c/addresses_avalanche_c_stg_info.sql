{% set blockchain = 'avalanche_c' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_info',
		materialized = 'view',
	)
}}

{{
	addresses_info_join(
		blockchain = blockchain,
		executed_txs_model = ref('addresses_avalanche_c_stg_executed_txs'),
		transfers_model = ref('addresses_avalanche_c_stg_transfers'),
		is_contract_model = ref('addresses_avalanche_c_stg_info_is_contract'),
	)
}}
