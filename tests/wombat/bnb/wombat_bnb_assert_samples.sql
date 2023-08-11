
with trades as (
    select
		block_time,
		token_bought_amount,
		token_sold_amount,
		tx_hash,
		evt_index
    from {{ ref('wombat_bnb_trades') }}
    where 1=0
		-- 3 manually tested swaps
		or (tx_hash=0xb228abac355fbc699ab0b134f48a495929069310aa32132ca5cdb679a81b6128 and evt_index=111)
		or (tx_hash=0x7a6f269ba6a5826e1ac578ec0bb3272429912ab38a8e2533706415cad6cf0634 and evt_index=128)
		or (tx_hash=0x478de9749640e880e80485550c62a63072fa946c791f3cf00b60e417c34cb4dd and evt_index=138)
)
, examples as (
    select * from {{ ref('dex_trades_seed') }}
	where blockchain = 'bnb' and project = 'wombat' and version = '1'
)
, matched as (
    select
        block_date
		, tr.tx_hash as tr_tx_hash
		, ex.tx_hash as ex_tx_hash
		, tr.evt_index as tr_evt_index
		, ex.evt_index as ex_evt_index
		, '|' as bounght_amount_test
		, case when abs(tr.token_bought_amount - ex.token_bought_amount) < 0.01 then true else false end as correct_bought_amount
		, tr.token_bought_amount as tr_token_bought_amount
		, ex.token_bought_amount as ex_token_bought_amount
		, tr.token_bought_amount - ex.token_bought_amount as delta_token_bought_amount
		, '|' as sold_amount_test
		, case when abs(tr.token_sold_amount - ex.token_sold_amount) < 0.01 then true else false end as correct_sold_amount
		, tr.token_sold_amount as tr_token_sold_amount
		, ex.token_sold_amount as ex_token_sold_amount
		, tr.token_sold_amount - ex.token_sold_amount as delta_token_sold_amount
    from trades tr
    full outer join examples ex
		on tr.tx_hash = ex.tx_hash and tr.evt_index = ex.evt_index
)
select * from matched
where not (correct_bought_amount and correct_sold_amount)
