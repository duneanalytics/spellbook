{{ config(
	schema='tokens_plasma',
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
	blockchain='plasma',
	traces=source('plasma', 'traces'),
	transactions=source('plasma', 'transactions'),
	erc20_transfers=source('erc20_plasma', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='plasma',
			transactions=source('plasma', 'transactions'),
			wrapped_token_deposit=source('wxpl_plasma', 'wxpl_evt_deposit'),
			wrapped_token_withdrawal=source('wxpl_plasma', 'wxpl_evt_withdrawal'),
		) }}
	)
