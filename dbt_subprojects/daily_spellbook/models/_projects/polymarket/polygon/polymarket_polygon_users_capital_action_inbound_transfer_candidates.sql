{{ config(
	schema='polymarket_polygon',
	alias='users_capital_action_inbound_transfer_candidates',
	materialized='view',
) }}

{% set inbound_transfer_candidate_models = [
	'polymarket_polygon_users_capital_action_inbound_usdce_transfer_candidates'
	, 'polymarket_polygon_users_capital_action_inbound_usdc_transfer_candidates'
	, 'polymarket_polygon_users_capital_action_inbound_pusd_transfer_candidates'
] -%}

{% for model in inbound_transfer_candidate_models -%}
select
	block_time
	, block_month
	, block_date
	, block_number
	, from_address
	, to_address
	, contract_address
	, symbol
	, amount_raw
	, amount
	, amount_usd
	, evt_index
	, tx_hash
from
	{{ ref(model) }}
{% if not loop.last -%}
union all
{% endif -%}
{% endfor -%}
