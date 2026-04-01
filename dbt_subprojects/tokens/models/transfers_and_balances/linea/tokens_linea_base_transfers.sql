{{ config(
	schema='tokens_linea',
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
	blockchain='linea',
	traces=source('linea', 'traces'),
	transactions=source('linea', 'transactions'),
	erc20_transfers=source('erc20_linea', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='linea',
			transactions=source('linea', 'transactions'),
			wrapped_token_deposit=source('linea_linea', 'weth9_evt_deposit'),
			wrapped_token_withdrawal=source('linea_linea', 'weth9_evt_withdrawal'),
		) }}
	)
