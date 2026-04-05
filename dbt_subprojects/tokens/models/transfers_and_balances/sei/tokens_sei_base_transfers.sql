{{ config(
	schema='tokens_sei',
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
	blockchain='sei',
	traces=source('sei', 'traces'),
	transactions=source('sei', 'transactions'),
	erc20_transfers=source('erc20_sei', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='sei',
			transactions=source('sei', 'transactions'),
			wrapped_token_deposit=source('wsei_sei', 'wsei_evt_deposit'),
			wrapped_token_withdrawal=source('wsei_sei', 'wsei_evt_withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='sei',
			transactions=source('sei', 'transactions'),
			erc20_transfers=source('erc20_sei', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_sei', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_sei', 'evt_withdraw'),
		) }}
	)
