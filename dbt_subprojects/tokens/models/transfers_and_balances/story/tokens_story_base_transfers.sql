{{ config(
	schema='tokens_story',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='table',
	tags=['static'],
	file_format='delta',
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
