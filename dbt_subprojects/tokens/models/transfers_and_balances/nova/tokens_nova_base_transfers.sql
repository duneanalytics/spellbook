{{ config(
	schema='tokens_nova',
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
	blockchain='nova',
	traces=source('nova', 'traces'),
	transactions=source('nova', 'transactions'),
	erc20_transfers=source('erc20_nova', 'evt_Transfer'),
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='nova',
			transactions=source('nova', 'transactions'),
			erc20_transfers=source('erc20_nova', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_nova', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_nova', 'evt_withdraw'),
		) }}
	)
