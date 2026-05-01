{{ config(
	schema='polymarket_polygon',
	alias='users_capital_actions',
	materialized='incremental',
	file_format='delta',
	incremental_strategy='merge',
	unique_key=['block_time', 'evt_index', 'tx_hash'],
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




with polymarket_addresses as (
	select distinct
		address
	from (
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
		select
			address
		from
			{{ source('polygon', 'creation_traces') }}
		where
			"from" = 0x8b9805a2f595b6705e74f7310829f2d299d21522 -- fpmm factory
	) as addresses
)
, polymarket_wallets as (
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
, token_transfers as (
	select
		t.block_time
		, t.block_date
		, t.block_number
		, t."from" as from_address
		, t."to" as to_address
		, t.contract_address
		, t.amount_raw
		, t.amount
		, t.amount_usd
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
		{% if target.name == 'ci' -%}
		and t.block_time >= now() - interval '7' day -- ci builds the model from scratch; bound the scan to fit trino hash limits
		{% endif -%}
		{% if is_incremental() -%}
		and {{ incremental_predicate('t.block_time') }}
		{% endif -%}
)
, classified as (
	select
		t.block_time
		, t.block_date
		, t.block_number
		, case
			when t.to_address = 0xd36ec33c8bed5a9f7b6630855f1533455b98a418
				and from_wallet.proxy is not null
				and t.contract_address in (
					0x2791bca1f2de4661ed88a30c99a7a9449aa84174
					, 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359
				)
				then 'convert'
			when t.from_address = 0xd36ec33c8bed5a9f7b6630855f1533455b98a418
				and to_wallet.proxy is not null
				and t.contract_address in (
					0x2791bca1f2de4661ed88a30c99a7a9449aa84174
					, 0x3c499c542cef5e3811e1192ce70d8cc03d5c3359
				)
				then 'convert'
			when to_wallet.proxy is not null
				and from_wallet.proxy is null
				and to_polymarket_address.address is null
				and from_polymarket_address.address is null
				then 'deposit'
			when from_wallet.proxy is not null
				and to_wallet.proxy is null
				and to_polymarket_address.address is null
				and from_polymarket_address.address is null
				then 'withdrawal'
			when from_wallet.proxy is not null
				and to_wallet.proxy is not null
				and to_polymarket_address.address is null
				and from_polymarket_address.address is null
				then 'transfer'
		end as action
		, t.from_address
		, t.to_address
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
		end as amount_usd -- pusd has no usd price feed yet; treat 1:1 with usdc
		, t.evt_index
		, t.tx_hash
	from
		token_transfers as t
	left join polymarket_wallets as to_wallet
		on t.to_address = to_wallet.proxy
	left join polymarket_wallets as from_wallet
		on t.from_address = from_wallet.proxy
	left join polymarket_addresses as to_polymarket_address
		on t.to_address = to_polymarket_address.address
	left join polymarket_addresses as from_polymarket_address
		on t.from_address = from_polymarket_address.address
)
, deduped as (
	select
		*
		, row_number() over (
			partition by
				block_time
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
