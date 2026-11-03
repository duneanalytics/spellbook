{{ config(
	schema = 'cleave_ethereum'
	, alias = 'base_trades'
	, materialized = 'incremental'
	, file_format = 'delta'
	, incremental_strategy = 'merge'
	, unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
	, incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
	)
}}

{{
	cleave_compatible_trades(
		blockchain = 'ethereum'
		, project_start_date = '2026-10-11'
		, swap_shells = ['0x016750b998aab0b4b07aa6a2276335053a798758']
		, measured_shells = ['0x5aff91027390f731058acc15e0a3f927b250c9fe', '0xed10296a4b2f4e51674e4dedca1f53dd9e0259f4']
		, routers = ['0x30809f82f8df91e976b02f09dc64e0da8efb1e92']
	)
}}
