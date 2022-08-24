BEGIN;
DROP VIEW IF EXISTS qidao.view_lp_pool_withdraw CASCADE;

CREATE VIEW qidao.view_lp_pool_withdraw AS (
with withdraws as (
/*select evt_block_time as block_time,
       user as user_address,
       contract_address as pool_contract_address,
       pid as lp_id,
       amount / 1e18 as amount,
       evt_tx_hash as tx_hash
from qidao."Farm_evt_Withdraw"
where amount > 0
union all*/
select block_time,
       '0x' || (SUBSTRING( encode(topic2, 'hex'), 25, 40)) AS user_address,
       contract_address as pool_contract_address,
       bytea2numeric( decode( SUBSTRING( encode(topic3, 'hex'), 1, 64), 'hex')) as lp_id,
       bytea2numeric( decode( SUBSTRING( encode(data, 'hex'), 1, 64), 'hex')) / 1e18 as amount,
       tx_hash
from polygon.logs
where block_time >= '2021-5-2'
  and contract_address in
      (select pool_contract_address
       from qidao.view_lp_pool_basic_info
      )
  and topic1 = '\xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568'
  and bytea2numeric( decode( SUBSTRING( encode(data, 'hex'), 1, 64), 'hex')) > 0
order by 1
)
select a."block_time", a."user_address", a."pool_contract_address",
       b."lp_contract_address", c."lp_name", a."amount", a.tx_hash
from withdraws a
     inner join qidao.view_lp_pool_basic_info b
       on a."pool_contract_address" = b."pool_contract_address"
          and a."lp_id" = b."lp_id"
     inner join qidao.view_lp_basic_info c
       on b."lp_contract_address" = c."lp_contract_address"
);

COMMIT;