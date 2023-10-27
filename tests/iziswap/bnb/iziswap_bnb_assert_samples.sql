
with trades as (
    select
		block_time,
		token_bought_amount,
		token_sold_amount,
		tx_hash,
		evt_index
    from {{ ref('iziswap_bnb_trades') }}
    where 1=0
		-- 3 manually tested swaps
		or (tx_hash=0x07e7b79ed2e7958c7744accc12ce8c732d82c6d62ce586365d276a72ecca8ba4 and evt_index=97)
		or (tx_hash=0x8c1e8cb4f4766ef9a775f040c576dd88a3a6993c8754c35f1e6faf4e1aa65718 and evt_index=197)
		or (tx_hash=0x57bbc0040e8db0bdf9dd38b1e888dfc124159a4ef4c0368426f89238db23d8de and evt_index=264)
)
, examples as (
    select * from {{ ref('dex_trades_seed') }}
	where blockchain = 'bnb' and project='iziswap' and version='1'
)
, matched as (
    select
        block_date
		, tr.tx_hash as tr_tx_hash
		, ex.tx_hash as ex_tx_hash
		, '|' as "|1|"
		, tr.evt_index as tr_evt_index
		, ex.evt_index as ex_evt_index
		, '|' as "|2|"
		, case when (tr.token_bought_amount - ex.token_bought_amount) < 0.01 then true else false end as correct_bought_amount
		, tr.token_bought_amount as tr_token_bought_amount
		, ex.token_bought_amount as ex_token_bought_amount
		, tr.token_bought_amount - ex.token_bought_amount as "Δ_token_bought_amount"
		, '|' as "|3|"
		, case when (tr.token_sold_amount - ex.token_sold_amount) < 0.01 then true else false end as correct_sold_amount
		, tr.token_sold_amount as tr_token_sold_amount
		, ex.token_sold_amount as ex_token_sold_amount
		, tr.token_sold_amount - ex.token_sold_amount as "Δ_token_sold_amount"
    from trades tr
    full outer join examples ex
		on tr.tx_hash=ex.tx_hash and tr.evt_index=ex.evt_index
)
select * from matched
where not (correct_bought_amount and correct_sold_amount)
