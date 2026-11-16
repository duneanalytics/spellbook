{{ config(
	schema='tokens_nova',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='table',
	tags=['static'],
	file_format='delta',
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
