{{ config(
	schema='tokens_hemi',
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
	blockchain='hemi',
	traces=source('hemi', 'traces'),
	transactions=source('hemi', 'transactions'),
	erc20_transfers=source('erc20_hemi', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='hemi',
			transactions=source('hemi', 'transactions'),
			wrapped_token_deposit=source('weth_hemi', 'weth9_evt_deposit'),
			wrapped_token_withdrawal=source('weth_hemi', 'weth9_evt_withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='hemi',
			transactions=source('hemi', 'transactions'),
			erc20_transfers=source('erc20_hemi', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_hemi', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_hemi', 'evt_withdraw'),
		) }}
	)
