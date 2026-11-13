{{ config(
	schema='tokens_taiko',
	alias='erc4626_self_minting_contracts',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['contract_address'],
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='taiko',
	erc20_transfers=source('erc20_taiko', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_taiko', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_taiko', 'evt_withdraw'),
) }}
