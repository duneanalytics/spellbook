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

{{ transfers_base(
	blockchain='ethereum',
	traces=source('ethereum', 'traces'),
	transactions=source('ethereum', 'transactions'),
	erc20_transfers=source('erc20_ethereum', 'evt_Transfer'),
) }}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='ethereum',
			transactions=source('ethereum', 'transactions'),
			wrapped_token_deposit=source('zeroex_ethereum', 'weth9_evt_deposit'),
			wrapped_token_withdrawal=source('zeroex_ethereum', 'weth9_evt_withdrawal'),
		) }}
	)

union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='ethereum',
			transactions=source('ethereum', 'transactions'),
			erc20_transfers=source('erc20_ethereum', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_ethereum', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_ethereum', 'evt_withdraw'),
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
