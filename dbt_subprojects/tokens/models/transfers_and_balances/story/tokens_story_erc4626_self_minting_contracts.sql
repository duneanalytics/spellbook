{{ config(
	schema='tokens_story',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='story',
	erc20_transfers=source('erc20_story', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_story', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_story', 'evt_withdraw'),
) }}
