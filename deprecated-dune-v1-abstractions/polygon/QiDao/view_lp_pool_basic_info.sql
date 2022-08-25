BEGIN;
DROP VIEW IF EXISTS qidao.view_lp_pool_basic_info CASCADE;

CREATE VIEW qidao.view_lp_pool_basic_info AS (
select contract_address as pool_contract_address,
       (row_number() over (partition by contract_address order by call_block_time)) - 1 as lp_id,
       "_lpToken" as lp_contract_address,
       "_allocPoint" as alloc_point,
       "_depositFeeBP"/10000 as deposit_fee_ratio
from qidao."Farm_call_add"
where call_success
union all
-- FarmV2 is not decoded
select '\x07Ca17Da3B54683f004d388F206269eF128C2356' as pool_contract_address,
       0 as lp_id,
       '\x447646e84498552e62eCF097Cc305eaBFFF09308' as lp_contract_address,
       45 as alloc_point,
       0.005 as deposit_fee_ratio
);

COMMIT;