{{ config(
	schema='tokens_avalanche_c',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

{{ transfers_base(
	blockchain='avalanche_c',
	traces=source('avalanche_c', 'traces'),
	transactions=source('avalanche_c', 'transactions'),
	erc20_transfers=source('erc20_avalanche_c', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='avalanche_c',
			transactions=source('avalanche_c', 'transactions'),
			wrapped_token_deposit=source('wavax_avalanche_c', 'wavax_evt_deposit'),
			wrapped_token_withdrawal=source('wavax_avalanche_c', 'wavax_evt_withdrawal'),
		) }}
	)
