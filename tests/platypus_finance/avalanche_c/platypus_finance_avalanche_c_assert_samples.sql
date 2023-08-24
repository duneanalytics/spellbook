
with trades as (
    select
		block_time,
		token_bought_amount,
		token_sold_amount,
		tx_hash,
		evt_index
    from {{ ref('platypus_finance_avalanche_c_trades') }}
    where 1=0
		-- 20 manually tested swaps
		or (tx_hash=0xc9cf002b6422ef0a617284537683372c66b92e84e0b28fde8a0cc04a4eef519e and evt_index=23)
		or (tx_hash=0x75dc4b71defb76d9888aabbd8771e8b38ee48fb41d43cc3ecae6fb73b3911c84 and evt_index=20)
		or (tx_hash=0x93bd89cf8a4d602d5cbc32446e5fb4bf9ed170f5ac72c2dc23294c8f5e1a8a05 and evt_index=26)
		or (tx_hash=0x306818d93ecd131c5e5e40a2293150db9484555d539a45e0512cc28a7041ebfb and evt_index=45)
		or (tx_hash=0x1bbe2f7773c059f500de2cd6acdee778d04b8c8185d4c69bb20db835feda9b76 and evt_index=24)
		or (tx_hash=0xd1d05ff16c664884875cf17ded334008fe1005b15103460c76a8979a791d3cc1 and evt_index=13)
		or (tx_hash=0x7fa1daa95f0c034752cb3719b62e7ae3d372945db163bda78e26cdc3a18192c3 and evt_index=6)
		or (tx_hash=0xd1e3319eb5ab9cae929d2a18e79facb10309f84a8e38073596df084745c2f2f1 and evt_index=113)
		or (tx_hash=0x916f2f560b3c13f31aa4139608b067e00ad042bd3bdb197a683e68d77c80aab0 and evt_index=12)
		or (tx_hash=0x9ac609a5cf6084d4152e835fcf790f45dbdb363bda79afc62ac3dfc5235eea7d and evt_index=17)
		or (tx_hash=0x2a3a710fa23fe85c0153a701167ac8265327e08c4e527e9de3d0727099934f48 and evt_index=8)
		or (tx_hash=0xa268e678b9167e2afbff624d6c473e1de4e69e00bbbf15cff30fec754965161e and evt_index=80)
		or (tx_hash=0x4587aca823136fd6dbe9ecf9a992074062c49d13ee38fc65a7f547e5647c40d6 and evt_index=34)
		or (tx_hash=0x2747e96f2e6198cbaad9a34257faae3d1282401ec89c4d8e2b8980830417f7e4 and evt_index=26)
		or (tx_hash=0x2954327b19870057c067530f4f951014163795afe991f5cd2657649ab8151a88 and evt_index=74)
		or (tx_hash=0x48596630bee61338ac47f100539d58fd783215ceb624d888249818b9c7eade5f and evt_index=59)
		or (tx_hash=0x138e58cdee3bd8ccb09bfaaffe340f84c29688d4a239dab4b94d12e49434d5a9 and evt_index=82)
		or (tx_hash=0x363e3d084f738cb84cc809ca6ec9738bc23ceb606ca13148fe210b0bb098115b and evt_index=22)
		or (tx_hash=0x70aa1e9d8f0698b0b4b118b5296d67a3fb15f380a8ca105ba3c433655ae93aa6 and evt_index=28)
		or (tx_hash=0x9e982c5f221d878d5c30291f3b1af3bb4896a0d15ecc305ff4d4e63936ed191e and evt_index=64)
)
, examples as (
    select * from {{ ref('dex_trades_seed') }}
	where blockchain = 'avalanche_c' and project='platypus_finance' and version='1'
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
