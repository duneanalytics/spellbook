
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
		or (tx_hash='0x5c4e8696f5ae333d1c2ee215b9533edfefefe533b243f67a118d89074faf1f69' and evt_index='95')
		or (tx_hash='0xa02ca83efdfc740d46788fda69aff0a44d809cf150834d1e03741d8a2f733d8a' and evt_index='121')
		or (tx_hash='0x106a1a6c98c61488201007b99b692b5ba5e73be9aadff85e9449855cfe7a216c' and evt_index='683')
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
