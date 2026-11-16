{{ config(
	schema='tokens_henesys',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='henesys',
	erc20_transfers=source('erc20_henesys', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_henesys', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_henesys', 'evt_withdraw'),
) }}
