{{ config(
	schema = 'cleave'
	, alias = 'base_trades'
	, materialized = 'view'
	, filtering_columns = ['block_date', 'blockchain']
	)
}}

{% set cleave_models = [
	ref('cleave_ethereum_base_trades')
	, ref('cleave_optimism_base_trades')
	, ref('cleave_arbitrum_base_trades')
	, ref('cleave_base_base_trades')
	, ref('cleave_bnb_base_trades')
	, ref('cleave_polygon_base_trades')
] -%}

select
	*
from (
	{% for cleave_model in cleave_models -%}
	select
		blockchain
		, project
		, version
		, block_month
		, block_date
		, block_time
		, token_bought_amount_raw
		, token_sold_amount_raw
		, token_bought_address
		, token_sold_address
		, taker
		, maker
		, project_contract_address
		, tx_hash
		, tx_from
		, tx_to
		, trace_address
		, evt_index
	from
		{{ cleave_model }}
	{% if not loop.last -%}
	union all
	{% endif -%}
	{% endfor -%}
)
