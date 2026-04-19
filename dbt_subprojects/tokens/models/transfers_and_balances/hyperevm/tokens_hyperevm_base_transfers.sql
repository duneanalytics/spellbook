{{ config(
	schema='tokens_hyperevm',
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
	blockchain='hyperevm',
	traces=source('hyperevm', 'traces'),
	transactions=source('hyperevm', 'transactions'),
	erc20_transfers=source('erc20_hyperevm', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='hyperevm',
			transactions=source('hyperevm', 'transactions'),
			wrapped_token_deposit=source('whype_hyperevm', 'weth9_evt_deposit'),
			wrapped_token_withdrawal=source('whype_hyperevm', 'weth9_evt_withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='hyperevm',
			transactions=source('hyperevm', 'transactions'),
			erc20_transfers=source('erc20_hyperevm', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_hyperevm', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_hyperevm', 'evt_withdraw'),
		) }}
	)
