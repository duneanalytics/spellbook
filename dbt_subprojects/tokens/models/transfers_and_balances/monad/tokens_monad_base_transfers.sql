{{ config(
	schema='tokens_monad',
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
	blockchain='monad',
	traces=source('monad', 'traces'),
	transactions=source('monad', 'transactions'),
	erc20_transfers=source('erc20_monad', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='monad',
			transactions=source('monad', 'transactions'),
			wrapped_token_deposit=source('wmon_monad', 'wmon_evt_deposit'),
			wrapped_token_withdrawal=source('wmon_monad', 'wmon_evt_withdrawal'),
		) }}
	)
