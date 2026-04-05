{{ config(
	schema='tokens_kaia',
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
	blockchain='kaia',
	traces=source('kaia', 'traces'),
	transactions=source('kaia', 'transactions'),
	erc20_transfers=source('erc20_kaia', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='kaia',
			transactions=source('kaia', 'transactions'),
			wrapped_token_deposit=source('wklay_kaia', 'wklay_evt_deposit'),
			wrapped_token_withdrawal=source('wklay_kaia', 'wklay_evt_withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='kaia',
			transactions=source('kaia', 'transactions'),
			erc20_transfers=source('erc20_kaia', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_kaia', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_kaia', 'evt_withdraw'),
		) }}
	)
