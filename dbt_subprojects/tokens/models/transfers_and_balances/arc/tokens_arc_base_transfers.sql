{{ config(
	schema='tokens_arc',
	alias='base_transfers',
	partition_by=['block_month'],
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	unique_key=['block_date', 'unique_key'],
	merge_skip_unchanged=true,
) }}

-- Arc's unpriced transfer stream. Written out rather than a transfers_base() call, and kept a
-- deliberate line-for-line analogue of dbt_subprojects/tokens/macros/transfers/transfers_base.sql
-- with that macro's parameters resolved to Arc's values, so the two can be diffed by eye. The
-- logic lives in the model because of the EIP-7708 handling below, which the macro has no
-- parameter for. Folding it back into the macro is a rewrite of the transfer logic and is left
-- as future work; every other chain keeps calling the macro unchanged.
--
-- Two things differ from the macro, both Arc-specific:
--
-- 1. No traces leg. Arc is the first chain on Dune to implement EIP-7708, which logs every
--    native value movement as a standard Transfer event from the system emitter at
--    0xffff...fffe (SYSTEM_ADDRESS = 2^160 - 2, fixed by the spec, not per-chain). That
--    stream is a complete native record on its own, so it replaces the traces leg entirely
--    rather than adding to it -- running both would double count. The emitter arrives on
--    erc20_arc.evt_Transfer alongside every other token, so it is classified inline in the
--    event projection instead of being read separately. This is also why there is no
--    tokens_arc_transfers_from_traces model.
--
-- 2. The 6-decimal USDC ERC-20 interface at 0x3600...0000 is excluded. The emitter already
--    carries every movement made through it, at native 18-decimal precision. It is dropped
--    by literal address rather than through the dune.blockchains join, so the deduplication
--    does not depend on that table's native metadata for Arc being any particular value.
--
-- Everything else -- column list, surrogate key, transaction join, incremental predicates --
-- is the macro's, unchanged. Both behaviours above are asserted by the singular tests in
-- dbt_subprojects/tokens/tests/arc/.

with transfers as (
	select
		cast(date_trunc('day', t.evt_block_time) as date) as block_date
		, t.evt_block_time as block_time
		, t.evt_block_number as block_number
		, t.evt_tx_hash as tx_hash
		, t.evt_index
		, cast(null as array<bigint>) as trace_address
		-- the d join cannot classify the emitter: its address is fixed by the spec and is
		-- never the chain's native token address, whatever dune.blockchains records.
		, case
			when t.contract_address = 0xfffffffffffffffffffffffffffffffffffffffe
			then (
				select
					token_address
				from
					{{ source('dune', 'blockchains') }}
				where
					name = 'arc'
			)
			else t.contract_address
		end as contract_address
		, case
			when t.contract_address = 0xfffffffffffffffffffffffffffffffffffffffe
			then 'native'
			when d.name is not null
			then 'native'
			else 'erc20'
		end as token_standard
		, t."from"
		, t.to
		, t.value as amount_raw
	from
		{{ source('erc20_arc', 'evt_Transfer') }} as t
	left join (
		{{ source('dune', 'blockchains') }}
	) as d
		on d.name = 'arc'
		and d.token_address = t.contract_address
	where
		-- every movement made through this interface is already carried by the emitter, at
		-- native precision. Excluded by literal address so the deduplication is independent
		-- of what dune.blockchains records as the chain's native token.
		t.contract_address != 0x3600000000000000000000000000000000000000
	{% if is_incremental() -%}
		and {{ incremental_predicate('t.evt_block_time') }}
	{% endif -%}
)

select
	{{ dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"]) }} as unique_key
	, 'arc' as blockchain
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
inner join {{ source('arc', 'transactions') }} as tx
	on tx.block_date = t.block_date
	and tx.block_number = t.block_number
	and tx.hash = t.tx_hash
{% if is_incremental() -%}
	and {{ incremental_predicate('tx.block_time') }}
{% endif -%}
