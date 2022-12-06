
with trades as (
    select
		block_time,
		token_bought_amount,
		token_sold_amount,
		tx_hash,
		evt_index
    from {{ ref('wombat_bnb_trades') }}
    where 1=0
		-- 6 manually tested swaps
		or (tx_hash='0x9b6a9e1b626bc607d296537c3eee92a3e1f51229938253b6685a6b002d710786' and evt_index='991')
		or (tx_hash='0x478de9749640e880e80485550c62a63072fa946c791f3cf00b60e417c34cb4dd' and evt_index='138')
		or (tx_hash='0xc016cb50b1044f767a0097b24501fadc72ff30840efe057fd2e3ee7812a4ad88' and evt_index='411')
		or (tx_hash='0xb228abac355fbc699ab0b134f48a495929069310aa32132ca5cdb679a81b6128' and evt_index='111')
		or (tx_hash='0x068e19b399f5bb6e25b0784ddadc5b5bd87d36fe14958f91c3ffaa67ed47dce1' and evt_index='91')
		or (tx_hash='0xc1cc6d3a674fd8472b1ce8f213e9d5b9e701e61eae14280a17bdab63a21b3fd9' and evt_index='358')
)
, examples as (
    select * from {{ ref('dex_trades_seed') }}
	where blockchain = 'bnb' and project='wombat' and version='1'
)
, matched as (
    select
        block_date
		, tr.tx_hash as tr_tx_hash
		, ex.tx_hash as ex_tx_hash
		, '|' as `|1|`
		, tr.evt_index as tr_evt_index
		, ex.evt_index as ex_evt_index
		, '|' as `|2|`
		, case when (tr.token_bought_amount - ex.token_bought_amount) < 0.01 then true else false end as correct_bought_amount
		, tr.token_bought_amount as tr_token_bought_amount 
		, ex.token_bought_amount as ex_token_bought_amount
		, tr.token_bought_amount - ex.token_bought_amount as `Δ token_bought_amount`
		, '|' as `|3|`
		, case when (tr.token_sold_amount - ex.token_sold_amount) < 0.01 then true else false end as correct_sold_amount
		, tr.token_sold_amount as tr_token_sold_amount
		, ex.token_sold_amount as ex_token_sold_amount
		, tr.token_sold_amount - ex.token_sold_amount as `Δ token_sold_amount`
    from trades tr
    full outer join examples ex
		on tr.tx_hash=ex.tx_hash and tr.evt_index=ex.evt_index
)
select * from matched
where not (correct_bought_amount and correct_sold_amount)
