{{ config(
	schema='tokens_somnia',
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
	blockchain='somnia',
	traces=source('somnia', 'traces'),
	transactions=source('somnia', 'transactions'),
	erc20_transfers=source('erc20_somnia', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='somnia',
			transactions=source('somnia', 'transactions'),
			wrapped_token_deposit=source('wsomi_somnia', 'wrappedsomi_evt_deposit'),
			wrapped_token_withdrawal=source('wsomi_somnia', 'wrappedsomi_evt_withdrawal'),
		) }}
	)
