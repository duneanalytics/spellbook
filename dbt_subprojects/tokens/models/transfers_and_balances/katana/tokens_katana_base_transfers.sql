{{ config(
	schema='tokens_katana',
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
	blockchain='katana',
	traces=source('katana', 'traces'),
	transactions=source('katana', 'transactions'),
	erc20_transfers=source('erc20_katana', 'evt_Transfer'),
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='katana',
			transactions=source('katana', 'transactions'),
			erc20_transfers=source('erc20_katana', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_katana', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_katana', 'evt_withdraw'),
		) }}
	)
