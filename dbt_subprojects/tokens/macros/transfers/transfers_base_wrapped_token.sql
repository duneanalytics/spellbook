{% macro transfers_base_wrapped_token(blockchain, transactions, wrapped_token_deposit, wrapped_token_withdrawal) %}
{% set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' %}
{% set default_address = '0x0000000000000000000000000000000000000000' %}

with transfers as (
	select
		t.evt_block_time as block_time
		, t.evt_block_number as block_number
		, t.evt_tx_hash as tx_hash
		, t.evt_index
		, cast(null as array<bigint>) as trace_address
		, t.contract_address
		, '{{ token_standard_20 }}' as token_standard
		, {{ default_address }} as "from"
		, t.dst as "to"
		, t.wad as amount_raw
	from
		{{ wrapped_token_deposit }} as t
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('t.evt_block_time') }}
	{% else -%}
	where
		{{ transfers_base_full_refresh_time_filter('t.evt_block_time') }}
	{% endif -%}

	union all

	select
		t.evt_block_time as block_time
		, t.evt_block_number as block_number
		, t.evt_tx_hash as tx_hash
		, t.evt_index
		, cast(null as array<bigint>) as trace_address
		, t.contract_address
		, '{{ token_standard_20 }}' as token_standard
		, t.src as "from"
		, {{ default_address }} as "to"
		, t.wad as amount_raw
	from
		{{ wrapped_token_withdrawal }} as t
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('evt_block_time') }}
	{% else -%}
	where
		{{ transfers_base_full_refresh_time_filter('t.evt_block_time') }}
	{% endif -%}
)

select
	{{ dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"]) }} as unique_key
	, '{{ blockchain }}' as blockchain
	, cast(date_trunc('month', t.block_time) as date) as block_month
	, cast(date_trunc('day', t.block_time) as date) as block_date
	, t.block_time
	, t.block_number
	, t.tx_hash
	, t.evt_index
	, t.trace_address
	, t.token_standard
	, tx."from" as tx_from
	, tx."to" as tx_to
	, tx."index" as tx_index
	, t."from"
	, t.to
	, t.contract_address
	, t.amount_raw
	, current_timestamp as _updated_at
from
	transfers as t
inner join {{ transactions }} as tx
	on tx.block_number = t.block_number
	and tx.hash = t.tx_hash
	{% if is_incremental() -%}
	and {{ incremental_predicate('tx.block_time') }}
	{% else -%}
	and {{ transfers_base_full_refresh_time_filter('tx.block_time') }}
	{% endif -%}
{% endmacro -%}
