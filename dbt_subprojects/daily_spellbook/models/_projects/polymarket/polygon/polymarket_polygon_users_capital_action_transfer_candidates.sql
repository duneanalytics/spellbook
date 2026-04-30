{{ config(
	schema='polymarket_polygon',
	alias='users_capital_action_transfer_candidates',
	materialized='view',
) }}

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

select
	t.block_time
	, t.block_month
	, t.block_date
	, t.block_number
	, t.from_address
	, t.to_address
	, t.contract_address
	, t.symbol
	, t.amount_raw
	, t.amount
	, t.amount_usd
	, t.evt_index
	, t.tx_hash
	, true as to_wallet
	, exists (
		select
			1
		from
			polymarket_wallets as from_wallet
		where
			t.from_address = from_wallet.proxy
	) as from_wallet
from
	{{ ref('polymarket_polygon_users_capital_action_inbound_transfer_candidates') }} as t
union all
select
	t.block_time
	, t.block_month
	, t.block_date
	, t.block_number
	, t.from_address
	, t.to_address
	, t.contract_address
	, t.symbol
	, t.amount_raw
	, t.amount
	, t.amount_usd
	, t.evt_index
	, t.tx_hash
	, false as to_wallet
	, true as from_wallet
from
	{{ ref('polymarket_polygon_users_capital_action_outbound_transfer_candidates') }} as t
where
	not exists (
		select
			1
		from
			polymarket_wallets as to_wallet
		where
			t.to_address = to_wallet.proxy
	)
