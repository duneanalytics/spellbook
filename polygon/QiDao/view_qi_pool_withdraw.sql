BEGIN;
DROP VIEW IF EXISTS qidao.view_qi_pool_withdraw CASCADE;

CREATE VIEW qidao.view_qi_pool_withdraw AS (
select evt_block_time as block_time,
       '0x' || encode("user", 'hex') as user_address,
       contract_address as pool_contract_address,
       amount/1e18 as amount,
       "endBlock" as end_block,
       evt_tx_hash as tx_hash
from qidao."eQi_evt_Leave"
);

COMMIT;