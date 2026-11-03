{{ config(
	schema = 'cleave_base'
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
		blockchain = 'base'
		, project_start_date = '2026-10-11'
		, swap_shells = ['0xb08f28a3575805ee1e3729fc19ab7e869cbaf563']
		, measured_shells = ['0x003d4c95756608ed4c11c5d0ad9ca4c4076caf20', '0x3a4569133c21013bbbc99a733fb891a7fffafd9a']
		, routers = ['0x545fef65c37ecee32309d879abafaa959e9f114c']
	)
}}
