BEGIN;
DROP VIEW IF EXISTS qidao.view_qi_staker_status CASCADE;

CREATE VIEW qidao.view_qi_staker_status AS (
select user_address, pool_contract_address,
       sum(locked_qi_change) as locked_qi,
       sum(end_block_number_change) as end_block_number
from qidao.view_qi_pool_events
group by 1,2
);

COMMIT;