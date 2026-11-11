{{ config(
	schema='tokens_degen',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='degen',
	erc20_transfers=source('erc20_degen', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_degen', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_degen', 'evt_withdraw'),
) }}
