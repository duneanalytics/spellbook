{% macro transfers_enrich(
	base_transfers = null
	, blockchain = null
	, transfers_start_date = null
	, tokens_erc20_model = source('tokens', 'erc20')
	, prices_model = source('prices', 'usd_with_native')
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

{#- Merge dest must see historical null-amount rows for recently updated tokens,
    otherwise incremental_predicates on block_date insert duplicate unique_keys. -#}
{%- set dest_pred -%}
({{ incremental_predicate('DBT_INTERNAL_DEST.block_date') }}
	or (
		DBT_INTERNAL_DEST.amount is null
		and DBT_INTERNAL_DEST.amount_raw is not null
		and DBT_INTERNAL_DEST.contract_address in (
			select e.contract_address
			from {{ tokens_erc20_model }} as e
			where e.decimals is not null
				and e.blockchain = '{{ blockchain }}'
				and {{ incremental_predicate('e._updated_at') }}
		)
	))
{%- endset -%}
{{- config(incremental_predicates = [dest_pred]) -}}

with
{% if is_incremental() %}
-- Re-enrich dest rows whose token metadata arrived after the incremental window.
recent_metadata as (
	select
		blockchain
		, contract_address
	from
		{{ tokens_erc20_model }} as e
	where
		e.decimals is not null
		and e.blockchain = '{{ blockchain }}'
		and {{ incremental_predicate('e._updated_at') }}
)
, heal_keys as (
	select
		d.block_month
		, d.block_date
		, d.unique_key
		, d.block_time
		, d.blockchain
		, d.contract_address
	from
		{{ this }} as d
	inner join recent_metadata as e
		on e.blockchain = d.blockchain
		and e.contract_address = d.contract_address
	where
		d.amount is null
		and d.amount_raw is not null
)
, {% endif %}
base_transfers as (
	select
		*
	from
		{{ base_transfers }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('block_date') }}
	union all
	select
		b.*
	from
		{{ base_transfers }} as b
	inner join heal_keys as h
		on b.block_month = h.block_month
		and b.block_date = h.block_date
		and b.unique_key = h.unique_key
	where
		not ({{ incremental_predicate('b.block_date') }})
	{% elif target.name == 'ci' -%}
	-- bound the CI initial-build scan to recent history so it completes against real data instead of
	-- scanning the full source range; prod and manual runs still use transfers_start_date for backfills.
	where
		block_date >= current_date - interval '7' day
	{% elif transfers_start_date is not none and transfers_start_date | trim != '' -%}
	where
		block_date >= date '{{ transfers_start_date }}'
	{% endif -%}
)
, prices as (
	select
		minute
		, blockchain
		, contract_address
		, decimals
		, symbol
		, price
	from
		{{ prices_model }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('minute') }}
	union all
	select
		p.minute
		, p.blockchain
		, p.contract_address
		, p.decimals
		, p.symbol
		, p.price
	from
		{{ prices_model }} as p
	inner join (
		select distinct
			date_trunc('minute', block_time) as minute
			, blockchain
			, contract_address
		from
			heal_keys
	) as h
		on p.minute = h.minute
		and p.blockchain = h.blockchain
		and p.contract_address = h.contract_address
	where
		not ({{ incremental_predicate('p.minute') }})
	{% elif target.name == 'ci' -%}
	where
		minute >= current_date - interval '7' day
	{% elif transfers_start_date is not none and transfers_start_date | trim != '' -%}
	where
		minute >= timestamp '{{ transfers_start_date }}'
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
		on date_trunc('minute', t.block_time) = prices.minute
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
