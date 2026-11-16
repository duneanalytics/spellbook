{{ config(
	schema='tokens_sophon',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='sophon',
	erc20_transfers=source('erc20_sophon', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_sophon', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_sophon', 'evt_withdraw'),
) }}
