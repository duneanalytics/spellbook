{{ config(
	schema='tokens_blast',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
	tags=['static'],
	post_hook='{{ hide_spells() }}',
) }}

{{ transfers_base(
	blockchain='blast',
	traces=source('blast', 'traces'),
	transactions=source('blast', 'transactions'),
	erc20_transfers=source('erc20_blast', 'evt_Transfer'),
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='blast',
			transactions=source('blast', 'transactions'),
			erc20_transfers=source('erc20_blast', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_blast', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_blast', 'evt_withdraw'),
		) }}
	)
