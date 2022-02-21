BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_lp_staker_status;

CREATE MATERIALIZED VIEW qidao.view_lp_staker_status as (
with dws as (
select block_time, user_address, lp_name,
       amount - fee_in_lp_token as change,
       fee_in_lp_token
from qidao.view_lp_pool_deposit
union all
select block_time, user_address, lp_name,
       amount * (-1) as change,
       0 as fee_in_lp_token
from qidao.view_lp_pool_withdraw
)
,user_status as (
select user_address,
       lp_name,
       sum(change) as staked_lp,
       sum(fee_in_lp_token) as fee_in_lp_token
from dws
group by 1, 2
)
--select * from user_status
select a."user_address",
       a."lp_name",
       a."staked_lp",
       a."staked_lp" * b."price" as staked_lp_value,
       a."fee_in_lp_token"
from user_status a inner join
     (select name as lp_name, price
      from qidao.view_lp_token_price
      order by day desc limit 5
     ) b
     on a."lp_name" = b."lp_name"
order by staked_lp_value desc
);

CREATE UNIQUE INDEX IF NOT EXISTS qidao_view_lp_staker_status_idx ON qidao.view_lp_staker_status (user_address, lp_name);

INSERT INTO cron.job(schedule, command)
VALUES ('15 */2 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_lp_staker_status$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;