{% macro transfers_enrich(
	base_transfers = null
	, blockchain = null
	, transfers_start_date = null
	, tokens_erc20_model = source('tokens', 'erc20')
	, prices_interval = 'hour'
	, trusted_tokens_model = source('prices', 'trusted_tokens')
	, usd_amount_threshold = 1000000000
	)
%}

{%- if blockchain is none or blockchain == '' -%}
	{{ exceptions.raise_compiler_error("blockchain parameter cannot be null or empty") }}
{%- endif -%}
{%- if base_transfers is none or base_transfers == '' -%}
	{{ exceptions.raise_compiler_error("base_transfers parameter cannot be null or empty") }}
{%- endif -%}

with base_transfers as (
	select
		*
	from
		{{ base_transfers }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('block_date') }}
	{% elif transfers_start_date is not none and transfers_start_date | trim != '' -%}
	where
		block_date >= date '{{ transfers_start_date }}'
	{% endif -%}
)
, prices as (
	select
		timestamp
		, blockchain
		, contract_address
		, decimals
		, symbol
		, price
	from
		{{ source('prices_external', prices_interval) }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('timestamp') }}
	{% elif transfers_start_date is not none and transfers_start_date | trim != '' -%}
	where
		timestamp >= timestamp '{{ transfers_start_date }}'
	{% endif -%}
)
, trusted_tokens as (
	select
		blockchain
		, contract_address
	from
		{{ trusted_tokens_model }}
)
, transfers as (
	select
		t.unique_key
		, t.blockchain
		, t.block_month
		, t.block_date
		, t.block_time
		, t.block_number
		, t.tx_hash
		, t.evt_index
		, t.trace_address
		, t.token_standard
		, t.tx_from
		, t.tx_to
		, t.tx_index
		, t."from"
		, t.to
		, t.contract_address
		, coalesce(tokens_erc20.symbol, prices.symbol) as symbol
		, t.amount_raw
		, t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) as amount
		, prices.price as price_usd
		, t.amount_raw / power(10, coalesce(tokens_erc20.decimals, prices.decimals)) * prices.price as amount_usd
		, case when trusted_tokens.blockchain is not null then true else false end as is_trusted_token
		, t._updated_at
	from
		base_transfers as t
	left join {{ tokens_erc20_model }} as tokens_erc20
		on tokens_erc20.blockchain = t.blockchain
		and tokens_erc20.contract_address = t.contract_address
	left join trusted_tokens
		on trusted_tokens.blockchain = t.blockchain
		and trusted_tokens.contract_address = t.contract_address
	left join prices
		on date_trunc('{{ prices_interval }}', t.block_time) = prices.timestamp
		and t.blockchain = prices.blockchain
		and t.contract_address = prices.contract_address
)
, final as (
	select
		unique_key
		, blockchain
		, block_month
		, block_date
		, block_time
		, block_number
		, tx_hash
		, evt_index
		, trace_address
		, token_standard
		, tx_from
		, tx_to
		, tx_index
		, "from"
		, to
		, contract_address
		, symbol
		, amount_raw
		, amount
		, price_usd
		, case
			when is_trusted_token = true then amount_usd
			when (is_trusted_token = false and amount_usd < {{ usd_amount_threshold }}) then amount_usd
			when (is_trusted_token = false and amount_usd >= {{ usd_amount_threshold }}) then cast(null as double)
			end as amount_usd
		, _updated_at
	from
		transfers
)
select
	*
from
	final
{%- endmacro %}
