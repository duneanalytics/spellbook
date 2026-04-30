{{ config(
	schema='polymarket_polygon',
	alias='market_addresses',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['address'],
) }}

-- Hardcoded protocol addresses + FPMM-factory deployments. Same single-key incremental
-- shape as users_proxies; no dest-side predicate for the same dupe-insert reason.

select distinct
	address
from (
	{% if not is_incremental() -%}
	select
		address
	from (
		values
		(0x4d97dcd97ec945f40cf65f87097ace5ea0476045) -- conditional tokens
		, (0x3a3bd7bb9528e159577f7c2e685cc81a765002e2) -- wrapped collateral
		, (0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e) -- ctfexchange (v1)
		, (0xc5d563a36ae78145c45a50134d48a1215220f80a) -- negriskctfexchange (v1)
		, (0xe111180000d2663c0091e4f400237545b87b996b) -- ctfexchange (v2, standard)
		, (0xe2222d279d744050d28e00520010520000310f59) -- ctfexchange (v2, negrisk)
		, (0xc288480574783bd7615170660d71753378159c47) -- polymarket rewards
		, (0x94a3db2f861b01c027871b08399e1ccecfc847f6) -- liq mining merkle distributor
		, (0xd36ec33c8bed5a9f7b6630855f1533455b98a418) -- usdc.e - usdc uniswap pool
	) as t(address)
	union all
	{% endif -%}
	select
		address
	from
		{{ source('polygon', 'creation_traces') }}
	where
		"from" = 0x8b9805a2f595b6705e74f7310829f2d299d21522 -- fpmm factory
		{% if is_incremental() -%}
		and {{ incremental_predicate('block_time') }}
		{% endif -%}
) as addresses
