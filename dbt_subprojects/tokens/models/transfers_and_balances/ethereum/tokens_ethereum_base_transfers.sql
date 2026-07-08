{{ config(
	schema='tokens_ethereum',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

with transfers as (
	select
		block_date
		, block_time
		, block_number
		, tx_hash
		, cast(null as bigint) as evt_index
		, trace_address
		, contract_address
		, 'native' as token_standard
		, "from"
		, "to" as to
		, amount_raw
	from
		{{ ref('tokens_ethereum_transfers_from_traces_base') }}
	where
		token_standard = 'native'
	{% if is_incremental() -%}
		and {{ incremental_predicate('block_time') }}
	{% endif -%}
	{% if target.name == 'ci' -%}
		-- CI-only scan bound (target=ci); prod/full-refresh unaffected.
		and block_date >= date(now() - interval '3' day)
		and block_time >= now() - interval '3' day
	{% endif -%}

	union all

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
			else 'erc20'
		end as token_standard
		, t."from"
		, t.to
		, t.value as amount_raw
	from
		{{ source('erc20_ethereum', 'evt_Transfer') }} as t
	left join (
		{{ source('dune', 'blockchains') }}
	) as d
		on d.name = 'ethereum'
		and d.token_address = t.contract_address
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('evt_block_time') }}
	{% elif target.name == 'ci' -%}
	where
		evt_block_date >= date(now() - interval '3' day)
		and evt_block_time >= now() - interval '3' day
	{% endif -%}
)

select
	{{ dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"]) }} as unique_key
	, 'ethereum' as blockchain
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
inner join {{ source('ethereum', 'transactions') }} as tx
	on tx.block_date = t.block_date
	and tx.block_number = t.block_number
	and tx.hash = t.tx_hash
	{% if is_incremental() -%}
	and {{ incremental_predicate('tx.block_time') }}
	{% endif -%}
	{% if target.name == 'ci' -%}
	and tx.block_date >= date(now() - interval '3' day)
	and tx.block_time >= now() - interval '3' day
	{% endif -%}

union all

{#- CI-only scan bound (target=ci); wraps whole-chain sources so macro-leg scans prune. Prod SQL renders bare subqueries; behavior unchanged. -#}
{%- set eth_tx -%}(select * from {{ source('ethereum', 'transactions') }}{% if target.name == 'ci' %} where block_date >= date(now() - interval '3' day){% endif %}){%- endset -%}
{%- set weth9_dep -%}(select * from {{ source('zeroex_ethereum', 'weth9_evt_deposit') }}{% if target.name == 'ci' %} where evt_block_date >= date(now() - interval '3' day){% endif %}){%- endset -%}
{%- set weth9_wit -%}(select * from {{ source('zeroex_ethereum', 'weth9_evt_withdrawal') }}{% if target.name == 'ci' %} where evt_block_date >= date(now() - interval '3' day){% endif %}){%- endset -%}
{%- set erc20_eth_ci -%}(select * from {{ source('erc20_ethereum', 'evt_Transfer') }}{% if target.name == 'ci' %} where evt_block_date >= date(now() - interval '3' day){% endif %}){%- endset -%}
{%- set erc4626_dep_eth -%}(select * from {{ source('erc4626_ethereum', 'evt_deposit') }}{% if target.name == 'ci' %} where evt_block_date >= date(now() - interval '3' day){% endif %}){%- endset -%}
{%- set erc4626_wit_eth -%}(select * from {{ source('erc4626_ethereum', 'evt_withdraw') }}{% if target.name == 'ci' %} where evt_block_date >= date(now() - interval '3' day){% endif %}){%- endset -%}

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='ethereum',
			transactions=eth_tx,
			wrapped_token_deposit=weth9_dep,
			wrapped_token_withdrawal=weth9_wit,
		) }}
	)

union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='ethereum',
			transactions=eth_tx,
			erc20_transfers=erc20_eth_ci,
			erc4626_deposit=erc4626_dep_eth,
			erc4626_withdraw=erc4626_wit_eth,
		) }}
	)

union all

select
	{{ dbt_utils.generate_surrogate_key(['w.block_number', "'withdrawal'", 'w.withdrawal_index']) }} as unique_key
	, 'ethereum' as blockchain
	, cast(date_trunc('month', w.block_time) as date) as block_month
	, cast(date_trunc('day', w.block_time) as date) as block_date
	, w.block_time
	, w.block_number
	, cast(null as varbinary) as tx_hash
	, cast(null as integer) as evt_index
	, cast(null as array(bigint)) as trace_address
	, 'native' as token_standard
	, cast(null as varbinary) as tx_from
	, cast(null as varbinary) as tx_to
	, cast(null as integer) as tx_index
	, 0x0000000000000000000000000000000000000000 as "from"
	, w.address as "to"
	, (
		select
			token_address
		from
			{{ source('dune', 'blockchains') }}
		where
			name = 'ethereum'
	) as contract_address
	, (cast(w.amount as uint256) * uint256 '1000000000') as amount_raw
	, current_timestamp as _updated_at
from
	(
		select
			block_time
			, block_number
			, "index" as withdrawal_index
			, address
			, amount
		from
			{{ source('ethereum', 'withdrawals') }}
		{% if target.name == 'ci' -%}
		where block_date >= date(now() - interval '3' day)
		{% endif -%}
	) as w
where
	w.address is not null
	and w.amount > 0
{% if is_incremental() -%}
	and {{ incremental_predicate('w.block_time') }}
{% endif -%}

union all

select
	{{ dbt_utils.generate_surrogate_key(['b.number', "'miner_reward'"]) }} as unique_key
	, 'ethereum' as blockchain
	, cast(date_trunc('month', b.time) as date) as block_month
	, b.date as block_date
	, b.time as block_time
	, b.number as block_number
	, cast(null as varbinary) as tx_hash
	, cast(null as integer) as evt_index
	, cast(null as array(bigint)) as trace_address
	, 'native' as token_standard
	, cast(null as varbinary) as tx_from
	, cast(null as varbinary) as tx_to
	, cast(null as integer) as tx_index
	, 0x0000000000000000000000000000000000000000 as "from"
	, b.miner as "to"
	, (
		select
			token_address
		from
			{{ source('dune', 'blockchains') }}
		where
			name = 'ethereum'
	) as contract_address
	, r.amount_raw
	, current_timestamp as _updated_at
from
	{{ source('ethereum', 'blocks') }} as b
inner join (
	select
		block_date
		, block_number
		, max(value) as amount_raw
	from
		{{ source('ethereum', 'traces') }}
	where
		type = 'reward'
		and value > uint256 '0'
		and block_number <= 15537393
		{% if is_incremental() -%}
		and {{ incremental_predicate('block_time') }}
		{% endif -%}
	group by
		1
		, 2
) as r
	on r.block_date = b.date
	and r.block_number = b.number
where
	b.number <= 15537393
{% if is_incremental() -%}
	and {{ incremental_predicate('b.time') }}
{% endif -%}
