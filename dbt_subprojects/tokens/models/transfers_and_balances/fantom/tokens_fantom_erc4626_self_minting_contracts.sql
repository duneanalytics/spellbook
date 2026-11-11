{{ config(
	schema='tokens_fantom',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='fantom',
	erc20_transfers=source('erc20_fantom', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_fantom', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_fantom', 'evt_withdraw'),
) }}
