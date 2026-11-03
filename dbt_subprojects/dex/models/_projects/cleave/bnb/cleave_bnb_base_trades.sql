{{ config(
	schema = 'cleave_bnb'
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
		blockchain = 'bnb'
		, project_start_date = '2026-10-11'
		, swap_shells = ['0x48dd089939c9a29211e7de9417e91a141d37f697']
		, measured_shells = ['0xc269a4f4debd4036597ee830349d824d5ba9ca7e', '0xe648d17e146c736a7296ba72260fb2ed5bb58c99']
		, routers = ['0x02ed7cd8d60768797328b098ce0df2fa99db6f23']
	)
}}
