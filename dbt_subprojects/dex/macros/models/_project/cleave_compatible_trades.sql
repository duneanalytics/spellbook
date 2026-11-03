{% macro cleave_compatible_trades(
	blockchain = null
	, project_start_date = null
	, swap_shells = []
	, measured_shells = []
	, routers = []
	)
%}

{% set swap_executed = '0xb0affd24a377c961919a05c43746ff51222eb9dd93d6185d47daf9bf66636972' -%}
{% set measured_swap_v1 = '0x0ceaa6d78e1718f1ee692841a049754d6b45967e71028cefa1274a864c46c6e2' -%}
{% if not (swap_shells or measured_shells) -%}
{{ exceptions.raise_compiler_error('cleave_compatible_trades needs at least one swap shell or measured shell address') }}
{% endif -%}

with shell_logs as (
	select
		l.block_time
		, l.block_number
		, l.contract_address
		, l.topic0
		, l.topic1
		, l.topic2
		, l.topic3
		, l.data
		, l.tx_hash
		, l.tx_from
		, l.tx_to
		, l.index as evt_index
	from
		{{ source(blockchain, 'logs') }} as l
	where
		l.block_date >= date '{{ project_start_date }}'
		and (
			{% if swap_shells -%}
			(
				l.topic0 = {{ swap_executed }}
				and l.contract_address in ({{ swap_shells | join(', ') }})
			)
			{% else -%}
			false
			{% endif -%}
			or {% if measured_shells -%}
			(
				l.topic0 = {{ measured_swap_v1 }}
				and l.contract_address in ({{ measured_shells | join(', ') }})
			)
			{% else -%}
			false
			{% endif -%}
		)
		{% if is_incremental() -%}
		and {{ incremental_predicate('l.block_time') }}
		{% endif -%}
)

, trades as (
	select
		s.block_time
		, s.block_number
		, '1' as version
		, varbinary_to_uint256(varbinary_substring(s.data, 33, 32)) as token_bought_amount_raw
		, varbinary_to_uint256(varbinary_substring(s.data, 1, 32)) as token_sold_amount_raw
		, varbinary_substring(s.topic2, 13, 20) as token_bought_address
		, varbinary_substring(s.topic1, 13, 20) as token_sold_address
		, s.tx_from as taker
		, s.contract_address
		, s.tx_hash
		, s.tx_from
		, s.tx_to
		, s.evt_index
	from
		shell_logs as s
	where
		s.topic0 = {{ swap_executed }}

	union all

	select
		s.block_time
		, s.block_number
		, '2' as version
		, varbinary_to_uint256(varbinary_substring(s.data, 161, 32)) as token_bought_amount_raw
		, varbinary_to_uint256(varbinary_substring(s.data, 33, 32)) as token_sold_amount_raw
		, varbinary_substring(s.data, 13, 20) as token_bought_address
		, varbinary_substring(s.topic3, 13, 20) as token_sold_address
		, case
			when varbinary_substring(s.topic1, 13, 20) = s.tx_from
				{% if routers -%}
				or varbinary_substring(s.topic2, 13, 20) in ({{ routers | join(', ') }})
				{% endif -%}
				then varbinary_substring(s.topic1, 13, 20)
			else varbinary_substring(s.topic2, 13, 20)
		end as taker
		, s.contract_address
		, s.tx_hash
		, s.tx_from
		, s.tx_to
		, s.evt_index
	from
		shell_logs as s
	where
		s.topic0 = {{ measured_swap_v1 }}
)

select
	'{{ blockchain }}' as blockchain
	, 'cleave' as project
	, t.version
	, cast(date_trunc('month', t.block_time) as date) as block_month
	, cast(date_trunc('day', t.block_time) as date) as block_date
	, t.block_time
	, t.block_number
	, t.token_bought_amount_raw
	, t.token_sold_amount_raw
	, t.token_bought_address
	, t.token_sold_address
	, t.taker
	, cast(null as varbinary) as maker
	, t.contract_address as project_contract_address
	, t.tx_hash
	, t.tx_from
	, t.tx_to
	, cast(array[-1] as array<bigint>) as trace_address
	, t.evt_index
from
	trades as t

{% endmacro %}
