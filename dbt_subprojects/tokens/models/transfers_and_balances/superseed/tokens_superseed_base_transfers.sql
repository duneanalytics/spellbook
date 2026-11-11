{{ config(
	schema='tokens_superseed',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_base(
	blockchain='superseed',
	traces=source('superseed', 'traces'),
	transactions=source('superseed', 'transactions'),
	erc20_transfers=source('erc20_superseed', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='superseed',
			transactions=source('superseed', 'transactions'),
			wrapped_token_deposit=source('weth_superseed', 'wrappedether_evt_deposit'),
			wrapped_token_withdrawal=source('weth_superseed', 'wrappedether_evt_withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='superseed',
			transactions=source('superseed', 'transactions'),
			erc20_transfers=source('erc20_superseed', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_superseed', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_superseed', 'evt_withdraw'),
		) }}
	)
