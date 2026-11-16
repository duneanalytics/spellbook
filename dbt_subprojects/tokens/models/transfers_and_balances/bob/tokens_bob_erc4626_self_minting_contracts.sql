{{ config(
	schema='tokens_bob',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='bob',
	erc20_transfers=source('erc20_bob', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_bob', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_bob', 'evt_withdraw'),
) }}
