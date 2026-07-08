{{ config(
	schema='tokens_bnb',
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
		{{ ref('tokens_bnb_transfers_from_traces_base') }}
	where
		token_standard = 'native'
	{% if is_incremental() -%}
		and {{ incremental_predicate('block_time') }}
	{% endif -%}
	{% if target.name == 'ci' -%}
		-- CI-only scan bound (target=ci); prod/full-refresh unaffected.
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
			else 'bep20'
		end as token_standard
		, t."from"
		, t.to
		, t.value as amount_raw
	from
		{{ source('erc20_bnb', 'evt_Transfer') }} as t
	left join (
		{{ source('dune', 'blockchains') }}
	) as d
		on d.name = 'bnb'
		and d.token_address = t.contract_address
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('evt_block_time') }}
	{% elif target.name == 'ci' -%}
	where
		evt_block_time >= now() - interval '3' day
	{% endif -%}
)

select
	{{ dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"]) }} as unique_key
	, 'bnb' as blockchain
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
inner join {{ source('bnb', 'transactions') }} as tx
	on tx.block_date = t.block_date
	and tx.block_number = t.block_number
	and tx.hash = t.tx_hash
	{% if is_incremental() -%}
	and {{ incremental_predicate('tx.block_time') }}
	{% endif -%}
	{% if target.name == 'ci' -%}
	and tx.block_time >= now() - interval '3' day
	{% endif -%}

union all

select
	*
from
	(
		{{ transfers_base_wrapped_token(
			blockchain='bnb',
			transactions=source('bnb', 'transactions'),
			wrapped_token_deposit=source('bnb_bnb', 'WBNB_evt_Deposit'),
			wrapped_token_withdrawal=source('bnb_bnb', 'WBNB_evt_Withdrawal'),
		) }}
	)
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='bnb',
			transactions=source('bnb', 'transactions'),
			erc20_transfers=source('erc20_bnb', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_bnb', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_bnb', 'evt_withdraw'),
		) }}
	)
