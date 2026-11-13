{{ config(
	schema='tokens_shape',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='shape',
	erc20_transfers=source('erc20_shape', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_shape', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_shape', 'evt_withdraw'),
) }}
