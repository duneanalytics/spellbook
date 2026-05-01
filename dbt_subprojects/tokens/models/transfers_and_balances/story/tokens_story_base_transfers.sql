{{ config(
	schema='tokens_story',
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
	blockchain='story',
	traces=source('story', 'traces'),
	transactions=source('story', 'transactions'),
	erc20_transfers=source('erc20_story', 'evt_Transfer'),
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='story',
			transactions=source('story', 'transactions'),
			erc20_transfers=source('erc20_story', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_story', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_story', 'evt_withdraw'),
		) }}
	)
