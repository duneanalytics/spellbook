{{ config(
	schema='tokens_zkevm',
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
	blockchain='zkevm',
	traces=source('zkevm', 'traces'),
	transactions=source('zkevm', 'transactions'),
	erc20_transfers=source('erc20_zkevm', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='zkevm',
			transactions=source('zkevm', 'transactions'),
			wrapped_token_deposit=source('weth_zkevm', 'weth9_evt_deposit'),
			wrapped_token_withdrawal=source('weth_zkevm', 'weth9_evt_withdrawal'),
		) }}
	)
