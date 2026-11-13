{{ config(
	schema='tokens_mantle',
	alias='erc4626_self_minting_contracts',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['contract_address'],
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='mantle',
	erc20_transfers=source('erc20_mantle', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_mantle', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_mantle', 'evt_withdraw'),
) }}
