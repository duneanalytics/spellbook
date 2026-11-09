{{ config(
	schema='tokens_peaq',
	alias='erc4626_self_minting_contracts',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['contract_address'],
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='peaq',
	erc20_transfers=source('erc20_peaq', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_peaq', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_peaq', 'evt_withdraw'),
) }}
