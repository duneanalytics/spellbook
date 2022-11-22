BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_lp_pool_harvest;

CREATE MATERIALIZED VIEW qidao.view_lp_pool_harvest AS (
with dws as (
select contract_address as pool_contract_address,
       bytea2numeric( decode( SUBSTRING( encode(topic3, 'hex'), 1, 64), 'hex')) as lp_id,
       tx_hash
from polygon.logs
where block_time >= '2021-5-3'
  and contract_address in
      (select distinct pool_contract_address
       from qidao.view_lp_pool_basic_info
      )
  and topic1 in  
    ('\x90890809c654f11d6e72a28fa60149770a0d11ec6c92319d6ceb2bb0a4ea1a15',
     '\xf279e6a1f5e320cca91135676d9cb6e44ca8a08c0b88342bcdb1144f6511b568')
order by 3
)
,dws_ext as(
select distinct pool_contract_address, lp_id, tx_hash from dws
)
select a.evt_block_time as block_time,
       '0x' || encode(a."to", 'hex') as user_address,
       c."lp_contract_address", d."lp_name",
       a."value" / 1e18 as amount, b."tx_hash"
from erc20."ERC20_evt_Transfer" a
     inner join dws_ext b
     on a."evt_block_time" >= '2021-5-3'
       and a."value" > 0
       and a."from" in
        (select distinct pool_contract_address
         from qidao.view_lp_pool_basic_info
        )
       and a."contract_address" = '\x580A84C73811E1839F75d86d75d88cCa0c241fF4'
       and a."evt_tx_hash" = b."tx_hash"
     inner join qidao.view_lp_pool_basic_info c
     on b."pool_contract_address" = c."pool_contract_address"
       and b."lp_id" = c."lp_id"
    inner join qidao.view_lp_basic_info d
       on c."lp_contract_address" = d."lp_contract_address"
);

INSERT INTO cron.job(schedule, command)
VALUES ('0 */2 * * *', $$REFRESH MATERIALIZED VIEW qidao.view_lp_pool_harvest$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;