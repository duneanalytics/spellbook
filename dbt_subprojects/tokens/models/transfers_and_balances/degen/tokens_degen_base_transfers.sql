{{ config(
	schema='tokens_degen',
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
	blockchain='degen',
	traces=source('degen', 'traces'),
	transactions=source('degen', 'transactions'),
	erc20_transfers=source('erc20_degen', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='degen',
			transactions=source('degen', 'transactions'),
			wrapped_token_deposit=source('wdegen_degen', 'wdegen_evt_deposit'),
			wrapped_token_withdrawal=source('wdegen_degen', 'wdegen_evt_withdrawal'),
		) }}
	)
