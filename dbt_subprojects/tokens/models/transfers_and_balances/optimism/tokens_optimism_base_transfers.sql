{{ config(
	schema='tokens_optimism',
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
	blockchain='optimism',
	traces=source('optimism', 'traces'),
	transactions=source('optimism', 'transactions'),
	erc20_transfers=source('erc20_optimism', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='optimism',
			transactions=source('optimism', 'transactions'),
			wrapped_token_deposit=source('weth_optimism', 'weth9_evt_deposit'),
			wrapped_token_withdrawal=source('weth_optimism', 'weth9_evt_withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='optimism',
			transactions=source('optimism', 'transactions'),
			erc20_transfers=source('erc20_optimism', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_optimism', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_optimism', 'evt_withdraw'),
		) }}
	)
