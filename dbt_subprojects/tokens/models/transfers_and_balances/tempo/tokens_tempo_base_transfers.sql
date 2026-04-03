{{ config(
	schema='tokens_tempo',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

with tip20_transfers as (
	select
		cast(date_trunc('day', t.evt_block_time) as date) as block_date
		, t.evt_block_time as block_time
		, t.evt_block_number as block_number
		, t.evt_tx_hash as tx_hash
		, t.evt_index
		, cast(null as array<bigint>) as trace_address
		, t.contract_address
		, 'tip20' as token_standard
		, t."from"
		, t.to
		, t.amount as amount_raw
	from
		{{ source('tip20_tempo', 'evt_Transfer') }} as t
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('evt_block_time') }}
	{% endif -%}
),
tip20_base as (
	select
		{{ dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"]) }} as unique_key
		, 'tempo' as blockchain
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
		tip20_transfers as t
	inner join {{ source('tempo', 'transactions') }} as tx
		on tx.block_date = t.block_date
		and tx.block_number = t.block_number
		and tx.hash = t.tx_hash
		{% if is_incremental() -%}
		and {{ incremental_predicate('tx.block_time') }}
		{% endif -%}
),
erc20_base as (
	select * from (
		{{ transfers_base(
			blockchain='tempo',
			traces=source('tempo', 'traces'),
			transactions=source('tempo', 'transactions'),
			erc20_transfers=source('erc20_tempo', 'evt_Transfer'),
			include_traces=false,
		) }}
	) as m
	where not exists (
		select 1
		from tip20_transfers as t
		where t.tx_hash = m.tx_hash
			and t.evt_index = m.evt_index
	)
)

select * from tip20_base

union all

select * from erc20_base
union all

select
	*
from
	(
		{{ transfers_base_erc4626(
			blockchain='tempo',
			transactions=source('tempo', 'transactions'),
			erc20_transfers=source('erc20_tempo', 'evt_Transfer'),
			erc4626_deposit=source('erc4626_tempo', 'evt_deposit'),
			erc4626_withdraw=source('erc4626_tempo', 'evt_withdraw'),
		) }}
	)
