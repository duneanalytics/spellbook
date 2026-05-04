{{ config(
	schema='polymarket_polygon',
	alias='users_proxies',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['proxy'],
) }}

-- Deduped Polymarket user proxy wallets across the Safe + magic.link factories. Materialized
-- so downstream `exists` references broadcast a small table instead of re-running the
-- union+distinct per reference site (Trino inlines CTEs).
-- No dest-side incremental_predicate: single-key merge would risk inserting duplicates.

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
