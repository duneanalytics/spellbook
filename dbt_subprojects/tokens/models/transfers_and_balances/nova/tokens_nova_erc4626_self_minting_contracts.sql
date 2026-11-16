{{ config(
	schema='tokens_nova',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='nova',
	erc20_transfers=source('erc20_nova', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_nova', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_nova', 'evt_withdraw'),
) }}
