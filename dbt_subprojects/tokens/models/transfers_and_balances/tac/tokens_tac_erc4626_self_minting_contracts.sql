{{ config(
	schema='tokens_tac',
	alias='erc4626_self_minting_contracts',
	materialized='table',
	tags=['static'],
	file_format='delta',
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='tac',
	erc20_transfers=source('erc20_tac', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_tac', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_tac', 'evt_withdraw'),
) }}
