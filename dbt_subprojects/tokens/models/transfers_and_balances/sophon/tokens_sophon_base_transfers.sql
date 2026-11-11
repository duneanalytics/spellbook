{{ config(
	schema='tokens_sophon',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

-- SOPH token on ZK Stack chain

{{ transfers_base(
	blockchain='sophon',
	traces=source('sophon', 'traces'),
	transactions=source('sophon', 'transactions'),
	erc20_transfers=source('erc20_sophon', 'evt_Transfer'),
	include_traces=false,
) }}
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='sophon',
			transactions=source('sophon', 'transactions'),
			erc20_transfers=source('erc20_sophon', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_sophon', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_sophon', 'evt_withdraw'),
		) }}
	)
