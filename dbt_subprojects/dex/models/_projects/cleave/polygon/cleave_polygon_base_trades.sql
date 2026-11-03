{{ config(
	schema = 'cleave_polygon'
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
		blockchain = 'polygon'
		, project_start_date = '2026-10-11'
		, swap_shells = ['0x5b4a223d8489a3b97d4ebe8fe93c6a7ac3ec1ad6']
		, measured_shells = ['0xa06ee4d57a2e6ea9213ab4bce2c055fd09bb43a1', '0x02ed7cd8d60768797328b098ce0df2fa99db6f23']
		, routers = ['0xea0e8cae2dba07d3bc828f42fd6219ff93ddce97']
	)
}}
