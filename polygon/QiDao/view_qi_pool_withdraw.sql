BEGIN;
DROP VIEW IF EXISTS dune_user_generated.qidao_view_qi_pool_withdraw CASCADE;

CREATE VIEW dune_user_generated.qidao_view_qi_pool_withdraw AS (
select evt_block_time as block_time,
       '0x' || encode("user", 'hex') as user_address,
       contract_address as pool_contract_address,
       amount/1e18 as amount,
       "endBlock" as end_block,
       evt_tx_hash as tx_hash
from qidao."eQi_evt_Leave"
);

COMMIT;
select * from dune_user_generated.qidao_view_qi_pool_withdraw order by block_time desc limit 1000