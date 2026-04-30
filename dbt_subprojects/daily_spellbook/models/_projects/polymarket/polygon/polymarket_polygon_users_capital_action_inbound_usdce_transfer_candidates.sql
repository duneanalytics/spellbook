{{ config(
	schema='polymarket_polygon',
	alias='users_capital_action_inbound_usdce_transfer_candidates',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	partition_by=['block_month'],
	unique_key=['block_month', 'block_time', 'evt_index', 'tx_hash'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
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
	, date_trunc('month', t.block_time) as block_month
	, t.block_date
	, t.block_number
	, t."from" as from_address
	, t."to" as to_address
	, t.contract_address
	, 'USDC.e' as symbol
	, t.amount_raw
	, t.amount
	, t.amount_usd
	, t.evt_index
	, t.tx_hash
from
	{{ source('tokens_polygon', 'transfers') }} as t
where
	t.contract_address = 0x2791bca1f2de4661ed88a30c99a7a9449aa84174 -- usdc.e
	and exists (
		select
			1
		from
			polymarket_wallets as to_wallet
		where
			t."to" = to_wallet.proxy
	)
	{% if is_incremental() -%}
	and {{ incremental_predicate('t.block_time') }}
	{% endif -%}
