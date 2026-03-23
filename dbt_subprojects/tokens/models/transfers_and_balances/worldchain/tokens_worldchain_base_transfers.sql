{{ config(
	schema='tokens_worldchain',
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
	blockchain='worldchain',
	traces=source('worldchain', 'traces'),
	transactions=source('worldchain', 'transactions'),
	erc20_transfers=source('erc20_worldchain', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='worldchain',
			transactions=source('worldchain', 'transactions'),
			wrapped_token_deposit=source('weth_worldchain', 'wrappedether_evt_deposit'),
			wrapped_token_withdrawal=source('weth_worldchain', 'wrappedether_evt_withdrawal'),
		) }}
	)
