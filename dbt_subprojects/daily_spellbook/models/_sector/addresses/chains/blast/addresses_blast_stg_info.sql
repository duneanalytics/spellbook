{% set blockchain = 'blast' %}

{{
	config(
		schema = 'addresses_' + blockchain,
		alias = 'stg_info',
		materialized = 'view',
		tags = ['static'],
		post_hook = '{{ hide_spells() }}',
	)
}}

{{
	addresses_info_join(
		blockchain = blockchain,
		executed_txs_model = ref('addresses_blast_stg_executed_txs'),
		transfers_model = ref('addresses_blast_stg_transfers'),
		is_contract_model = ref('addresses_blast_stg_info_is_contract'),
	)
}}
