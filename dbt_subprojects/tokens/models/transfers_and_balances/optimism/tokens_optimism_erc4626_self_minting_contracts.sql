{{ config(
	schema='tokens_optimism',
	alias='erc4626_self_minting_contracts',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['contract_address'],
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='optimism',
	erc20_transfers=source('erc20_optimism', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_optimism', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_optimism', 'evt_withdraw'),
) }}
