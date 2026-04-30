{{ config(
	schema='polymarket_polygon',
	alias='users_capital_actions',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	partition_by=['block_month'],
	unique_key=['block_month', 'block_time', 'evt_index', 'tx_hash'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
	post_hook='{{ hide_spells() }}',
) }}

-- lots of edge cases here to ensure that we're just picking up on actual deposits and not internal transfers
-- this is a bit of a mess, but it works for now

-- we look for usdc.e and usdc transfers
-- usdc.e is the wrapped version of usdc on polygon polymarket runs on this
-- if you deposit using usdc, the UI will prompt you to wrap your USDC into USDC.e by signing a message
-- this will just use uniswap to swap your usdc for usdc.e, so we need to exclude 0xD36ec33c8bed5a9F7B6630855f1533455b98a418 as this is the uniswap pool
-- by ignoring the uniswap pool, but looking for USDC transfers, we can get a better read on funding sources




with transfer_candidates as (
	select
		t.block_time
		, t.block_month
		, t.block_date
		, t.block_number
		, t.from_address
		, t.to_address
		, t.symbol
		, t.amount_raw
		, t.amount
		, t.amount_usd
		, t.evt_index
		, t.tx_hash
		, t.to_wallet
		, t.from_wallet
		, exists (
			select
				1
			from
				{{ ref('polymarket_polygon_market_addresses') }} as to_polymarket_address
			where
				t.to_address = to_polymarket_address.address
		) as to_polymarket_address
		, exists (
			select
				1
			from
				{{ ref('polymarket_polygon_market_addresses') }} as from_polymarket_address
			where
				t.from_address = from_polymarket_address.address
		) as from_polymarket_address
	from
		{{ ref('polymarket_polygon_users_capital_action_transfer_candidates') }} as t
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('t.block_time') }}
	{% endif -%}
)
, classified as (
	select
		t.block_time
		, t.block_month
		, t.block_date
		, t.block_number
		, case
			when t.to_address = 0xd36ec33c8bed5a9f7b6630855f1533455b98a418
				and t.from_wallet
				and t.symbol in ('USDC.e', 'USDC')
				then 'convert'
			when t.from_address = 0xd36ec33c8bed5a9f7b6630855f1533455b98a418
				and t.to_wallet
				and t.symbol in ('USDC.e', 'USDC')
				then 'convert'
			when t.to_wallet
				and not t.from_wallet
				and not t.to_polymarket_address
				and not t.from_polymarket_address
				then 'deposit'
			when t.from_wallet
				and not t.to_wallet
				and not t.to_polymarket_address
				and not t.from_polymarket_address
				then 'withdrawal'
			when t.from_wallet
				and t.to_wallet
				and not t.to_polymarket_address
				and not t.from_polymarket_address
				then 'transfer'
		end as action
		, t.from_address
		, t.to_address
		, t.symbol
		, t.amount_raw
		, t.amount
		, t.amount_usd
		, t.evt_index
		, t.tx_hash
	from
		transfer_candidates as t
)
, deduped as (
	select
		*
		, row_number() over (
			partition by
				block_time
				, block_month
				, block_date
				, block_number
				, action
				, from_address
				, to_address
				, symbol
				, amount_raw
				, amount
				, amount_usd
				, evt_index
				, tx_hash
			order by
				tx_hash
		) as duplicate_rank
	from
		classified
	where
		action is not null
)

select
	block_time
	, block_month
	, block_date
	, block_number
	, action
	, from_address
	, to_address
	, symbol
	, amount_raw
	, amount
	, amount_usd
	, evt_index
	, tx_hash
from
	deduped
where
	action != 'transfer'
	or duplicate_rank = 1
