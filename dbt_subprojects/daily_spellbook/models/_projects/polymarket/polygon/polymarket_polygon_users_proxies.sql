{{ config(
	schema='polymarket_polygon',
	alias='users_proxies',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['proxy'],
	incremental_predicates=[incremental_predicate('DBT_INTERNAL_DEST.block_time')],
) }}

-- Deduped Polymarket user proxy wallets across both factories (Safe + magic.link).
-- Materialized to a single Delta table so downstream models (e.g. users_capital_actions)
-- can scan and broadcast it once per query, instead of re-expanding the union+distinct
-- inside every `exists` subquery (Trino inlines CTEs by default, which causes the
-- aggregation to run once per reference site).
--
-- A given proxy address is created exactly once on-chain, but we group on proxy + take
-- the earliest block_time defensively in case of any cross-factory collision.

select
	proxy
	, min(block_time) as block_time
from (
	select
		proxy
		, block_time
	from
		{{ ref('polymarket_polygon_users_safe_proxies') }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('block_time') }}
	{% endif -%}
	union all
	select
		proxy
		, block_time
	from
		{{ ref('polymarket_polygon_users_magic_wallet_proxies') }}
	{% if is_incremental() -%}
	where
		{{ incremental_predicate('block_time') }}
	{% endif -%}
) as w
group by
	proxy
