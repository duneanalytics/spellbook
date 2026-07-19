{% set chains = gas_evm_chains() %}

{{ config(
	schema = 'gas_evm'
	, alias = 'fees'
	, materialized = 'view'
	, post_hook = '{{ expose_spells(blockchains = \'["' + chains | join('","') + '"]\',
									spell_type = "sector",
									spell_name = "gas_evm",
									contributors = \'["soispoke", "ilemi", "0xRob", "jeff-dude", "krishhh", "tomfutago"]\') }}'
	)
}}

select
	blockchain
	, block_month
	, block_date
	, block_time
	, block_number
	, tx_hash
	, tx_index
	, tx_from
	, tx_to
	, gas_price
	, gas_used
	, currency_symbol
	, tx_fee
	, tx_fee_usd
	, tx_fee_raw
	, tx_fee_breakdown
	, tx_fee_breakdown_usd
	, tx_fee_breakdown_raw
	, tx_fee_currency
	, block_proposer
	, gas_limit
	, gas_limit_usage
from {{ ref('gas_fees') }}
where blockchain != 'tron'
