{% macro transfers_base(blockchain, traces, transactions, erc20_transfers, include_traces=true) %}
{% set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' %}

with transfers as (
{% if include_traces -%}
	select
		block_date
		, block_time
		, block_number
		, tx_hash
		, cast(null as bigint) as evt_index
		, trace_address
		, (
			select
				token_address
			from
				{{ source('dune', 'blockchains') }}
			where
				name = '{{ blockchain }}'
		) as contract_address
		, 'native' as token_standard
		, "from"
		, coalesce(to, address) as to
		, value as amount_raw
	from
		{{ traces }}
	where
		success
		and (
			call_type not in ('delegatecall', 'callcode', 'staticcall')
			or call_type is null
		)
		and value > uint256 '0'
	{% if is_incremental() -%}
		and {{ incremental_predicate('block_time') }}
	{% else -%}
		and {{ transfers_base_full_refresh_time_filter('block_time') }}
	{% endif -%}
	{% if blockchain == 'polygon' -%}
		and case
			when
				to = (
					select
						token_address
					from
						{{ source('dune', 'blockchains') }}
					where
						name = '{{ blockchain }}'
				)
				or "from" = (
					select
						token_address
					from
						{{ source('dune', 'blockchains') }}
					where
						name = '{{ blockchain }}'
				)
			then false
			else true
		end
	{% endif -%}

	union all
{% endif -%}

	select
		cast(date_trunc('day', t.evt_block_time) as date) as block_date
		, t.evt_block_time as block_time
		, t.evt_block_number as block_number
		, t.evt_tx_hash as tx_hash
		, t.evt_index
		, cast(null as array<bigint>) as trace_address
		, t.contract_address
		, case
			when d.name is not null
			then 'native'
			else '{{ token_standard_20 }}'
		end as token_standard
		, t."from"
		, t.to
		, t.value as amount_raw
	from
		{{ erc20_transfers }} as t
	left join (
		{{ source('dune', 'blockchains') }}
	) as d
		on d.name = '{{ blockchain }}'
		and d.token_address = t.contract_address
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
	, cast(date_trunc('month', t.block_date) as date) as block_month
	, t.block_date
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
	on
	{% if blockchain == 'gnosis' -%}
	cast(date_trunc('day', tx.block_time) as date) = t.block_date
	{% else -%}
	tx.block_date = t.block_date
	{% endif -%}
	and tx.block_number = t.block_number
	and tx.hash = t.tx_hash
	{% if is_incremental() -%}
	and {{ incremental_predicate('tx.block_time') }}
	{% else -%}
	and {{ transfers_base_full_refresh_time_filter('tx.block_time') }}
	{% endif -%}
{% endmacro -%}
