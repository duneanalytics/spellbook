BEGIN;
DROP VIEW IF EXISTS qidao.view_qi_pool_deposit CASCADE;

CREATE VIEW qidao.view_qi_pool_deposit AS (
with dws as (
select evt_block_time as block_time,
       '0x' || encode("user", 'hex') as user_address,
       contract_address as pool_contract_address,
       amount as amount,
       "endBlock" as end_block,
       evt_tx_hash as tx_hash,
       'deposit' as "type"
from qidao."eQi_evt_Enter"
union all
select evt_block_time as block_time,
       '0x' || encode("user", 'hex') as user_address,
       contract_address as pool_contract_address,
       0 as amount,
       0 as end_block,
       evt_tx_hash as tx_hash,
       'withdraw' as "type"
from qidao."eQi_evt_Leave"
)
,dws_ag as (
select block_time, user_address, pool_contract_address,
       (amount - (lag(amount, 1, 0::numeric) over (partition by user_address, pool_contract_address order by block_time)))/1e18 as amount,
       end_block - lag(end_block, 1, 0::numeric) over (partition by user_address, pool_contract_address order by block_time) as end_block_number_added,
       tx_hash, "type"
from dws
order by block_time
)
select block_time, user_address, pool_contract_address, amount,
       end_block_number_added, tx_hash
from dws_ag
where "type" = 'deposit'
);

COMMIT;