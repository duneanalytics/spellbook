{{ config(
	schema='polymarket_polygon',
	alias='users_capital_actions',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='microbatch',
	event_time='block_date',
	begin='2020-09-27',
	batch_size=var('polymarket_polygon_capital_actions_batch_size', 'day'),
	lookback=3,
	partition_by=['block_month'],
	unique_key=['block_month', 'block_time', 'evt_index', 'tx_hash'],
	tags=['microbatch'],
) }}

-- Polymarket user capital actions on Polygon: deposits, withdrawals, internal transfers,
-- and USDC <-> USDC.e conversions through the canonical Uniswap pool.
--
-- Token coverage:
--   - USDC.e (V1 collateral) and USDC (deposit-side) drive 'deposit'/'withdrawal'/'transfer'/'convert'
--   - pUSD (V2 collateral; 1:1 USDC-backed; no USD price feed) treated 1:1 with USDC for amount_usd
-- The canonical USDC <-> USDC.e Uniswap pool (0xd36e...a418) is excluded from deposit/withdrawal
-- classification and tagged as 'convert'.
--
-- This model is microbatched on block_date so each run plans only its own day window of
-- tokens_polygon.transfers against the ~7M-row Polymarket wallet set, avoiding the
-- multi-year partitioned-hash explosion seen on full-history single-shot builds.

with polymarket_wallets as (
	select distinct
		proxy
	from (
		select
			proxy
		from
			{{ ref('polymarket_polygon_users_magic_wallet_proxies') }}
		union all
		select
			proxy
		from
			{{ ref('polymarket_polygon_users_safe_proxies') }}
	) as wallets
)
, transfers as (
	select
		t.block_time
		, date_trunc('month', t.block_time) as block_month
		, t.block_date
		, t.block_number
		, t."from" as from_address
		, t."to" as to_address
		, t.contract_address
		, case
			when t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 then 'USDC.e'
			when t.contract_address = 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359 then 'USDC'
			when t.contract_address = 0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb then 'pUSD'
		end as symbol
		, t.amount_raw
		, t.amount
		, case
			when t.contract_address = 0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb then t.amount
			else t.amount_usd
		end as amount_usd
		, t.evt_index
		, t.tx_hash
	from
		{{ source('tokens_polygon', 'transfers') }} as t
	where
		t.contract_address in (
			0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- usdc.e
			, 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359 -- usdc
			, 0xc011a7e12a19f7b1f670d46f03b03f3342e82dfb -- pusd (v2 collateral; 1:1 usdc-backed)
		)
)
, transfer_candidates as (
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
		, exists (
			select
				1
			from
				polymarket_wallets as to_wallet
			where
				t.to_address = to_wallet.proxy
		) as to_wallet
		, exists (
			select
				1
			from
				polymarket_wallets as from_wallet
			where
				t.from_address = from_wallet.proxy
		) as from_wallet
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
		transfers as t
	where
		exists (
			select
				1
			from
				polymarket_wallets as w
			where
				t.to_address = w.proxy
				or t.from_address = w.proxy
		)
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
		c.block_time
		, c.block_month
		, c.block_date
		, c.block_number
		, c.action
		, c.from_address
		, c.to_address
		, c.symbol
		, c.amount_raw
		, c.amount
		, c.amount_usd
		, c.evt_index
		, c.tx_hash
		, row_number() over (
			partition by
				c.block_time
				, c.block_month
				, c.block_date
				, c.block_number
				, c.action
				, c.from_address
				, c.to_address
				, c.symbol
				, c.amount_raw
				, c.amount
				, c.amount_usd
				, c.evt_index
				, c.tx_hash
			order by
				c.tx_hash
		) as duplicate_rank
	from
		classified as c
	where
		c.action is not null
)

select
	d.block_time
	, d.block_month
	, d.block_date
	, d.block_number
	, d.action
	, d.from_address
	, d.to_address
	, d.symbol
	, d.amount_raw
	, d.amount
	, d.amount_usd
	, d.evt_index
	, d.tx_hash
from
	deduped as d
where
	d.action != 'transfer'
	or d.duplicate_rank = 1
