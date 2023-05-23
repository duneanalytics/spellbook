with  tests as (
select  'Check Lido deposits' as test_name, 
case when sum(amount_staked) = 224000000000000000000 then true else false end as success
from {{ref('lido_accounting_ethereum_deposits')}}
where tx_hash = lower('0xada597e877c290f1f942f2c13820f5a7c584ad56b84e71ccf053ecab81c54b4b')
union all
select 'Check Lido revenue' as test_name, 
case when total = 709756144576347800000 then true else false end as success
from {{ref('lido_accounting_ethereum_revenue')}}
where evt_tx_hash = lower('0x43c5987e0283da0184e66497fbccaffa9c2fdf2be876abc28c9c65bead5a7c89')
)



select *
from tests
where success is false