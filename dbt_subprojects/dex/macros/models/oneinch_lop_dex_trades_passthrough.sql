{% macro oneinch_lop_dex_trades_passthrough(
	blockchain
) %}

{%- if blockchain is none or blockchain == '' -%}
	{{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}

SELECT
	o.blockchain
	, o.project
	, o.version
	, o.block_month
	, o.block_date
	, o.block_time
	, o.block_number
	, o.token_bought_symbol
	, o.token_sold_symbol
	, o.token_pair
	, o.token_bought_amount
	, o.token_sold_amount
	, o.token_bought_amount_raw
	, o.token_sold_amount_raw
	, o.amount_usd
	, o.token_bought_address
	, o.token_sold_address
	, o.taker
	, o.maker
	, o.project_contract_address
	, o.tx_hash
	, o.tx_from
	, o.tx_to
	, o.evt_index
FROM {{ ref('oneinch_lop_own_trades') }} AS o
WHERE o.blockchain = '{{ blockchain }}'

{% endmacro %}
