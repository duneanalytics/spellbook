{{ config(
	schema='tokens_boba',
	alias='erc4626_self_minting_contracts',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['contract_address'],
) }}

{{ transfers_erc4626_self_minting_contracts(
	blockchain='boba',
	erc20_transfers=source('erc20_boba', 'evt_Transfer'),
	erc4626_deposit=source('erc4626_boba', 'evt_deposit'),
	erc4626_withdraw=source('erc4626_boba', 'evt_withdraw'),
) }}
