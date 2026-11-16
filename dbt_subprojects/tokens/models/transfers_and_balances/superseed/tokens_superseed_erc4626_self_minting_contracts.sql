{{ config(
	schema='tokens_superseed',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='superseed',
	erc20_transfers=source('erc20_superseed', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_superseed', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_superseed', 'evt_withdraw'),
) }}
