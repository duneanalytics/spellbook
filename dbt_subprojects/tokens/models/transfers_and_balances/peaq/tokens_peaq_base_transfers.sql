{{ config(
	schema='tokens_peaq',
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
	blockchain='peaq',
	traces=source('peaq', 'traces'),
	transactions=source('peaq', 'transactions'),
	erc20_transfers=source('erc20_peaq', 'evt_Transfer'),
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='peaq',
			transactions=source('peaq', 'transactions'),
			erc20_transfers=source('erc20_peaq', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_peaq', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_peaq', 'evt_withdraw'),
		) }}
	)
