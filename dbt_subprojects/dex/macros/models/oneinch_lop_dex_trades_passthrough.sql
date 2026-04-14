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
	, coalesce(tb.symbol, o.token_bought_symbol) as token_bought_symbol
	, coalesce(ts.symbol, o.token_sold_symbol) as token_sold_symbol
	, case
		when lower(coalesce(tb.symbol, o.token_bought_symbol)) > lower(coalesce(ts.symbol, o.token_sold_symbol)) then concat(coalesce(ts.symbol, o.token_sold_symbol), '-', coalesce(tb.symbol, o.token_bought_symbol))
		else concat(coalesce(tb.symbol, o.token_bought_symbol), '-', coalesce(ts.symbol, o.token_sold_symbol))
	end as token_pair
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
LEFT JOIN {{ source('tokens', 'erc20') }} AS tb
	ON tb.blockchain = '{{ blockchain }}'
	AND tb.contract_address = o.token_bought_address
LEFT JOIN {{ source('tokens', 'erc20') }} AS ts
	ON ts.blockchain = '{{ blockchain }}'
	AND ts.contract_address = o.token_sold_address
WHERE o.blockchain = '{{ blockchain }}'
{% if var('dev_dates', false) -%}
	AND o.block_date > current_date - interval '3' day -- dev_dates mode for dev, to prevent full scan
{%- else -%}
	{% if is_incremental() %}
	AND {{ incremental_predicate('o.block_time') }}
	{% endif %}
{%- endif %}

{% endmacro %}
