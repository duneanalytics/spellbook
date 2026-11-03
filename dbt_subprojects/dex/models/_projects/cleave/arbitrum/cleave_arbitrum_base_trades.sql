{{ config(
	schema = 'cleave_arbitrum'
	, alias = 'base_trades'
	, materialized = 'incremental'
	, file_format = 'delta'
	, incremental_strategy = 'merge'
	, unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
	, incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
	, filtering_columns = ['block_date']
	)
}}

{{
	cleave_compatible_trades(
		blockchain = 'arbitrum'
		, project_start_date = '2026-10-11'
		, swap_shells = ['0x4052700f83324be5710d187b7bbea9837dffa8fc']
		, measured_shells = ['0x78d851d8bc6a9b9739549e7515bf02d6394e1794', '0x02ed7cd8d60768797328b098ce0df2fa99db6f23']
		, routers = ['0xea0e8cae2dba07d3bc828f42fd6219ff93ddce97']
	)
}}
