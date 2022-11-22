BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_lp_pool_daily_status;

CREATE MATERIALIZED VIEW qidao.view_lp_pool_daily_status as (
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
,changes_daily as (
select date_trunc('day', block_time) as day,
       lp_name,
       sum(fee_in_lp_token) as fee_in_lp_token,
       sum(change) as staked_lp_change
from dws
group by 1,2
)
,changes_daily_ext as (
select a."day", a."lp_name",
       a."staked_lp_change",
       a."staked_lp_change" * b."price" as staked_lp_value_change,
       a."fee_in_lp_token",
       a."fee_in_lp_token" * b."price" as fee_in_usd,
       sum(a."staked_lp_change") over (partition by a."lp_name" order by a."day") as cumulative_staked_lp_token,
       (sum(a."staked_lp_change") over (partition by a."lp_name" order by a."day")) * b."price" as cumulative_staked_lp_value,
       sum(a."fee_in_lp_token") over (partition by a."lp_name" order by a."day") as cumulative_fee_in_lp_token,
       sum(a."fee_in_lp_token" * b."price") over (partition by a."lp_name" order by a."day") as cumulative_fee_in_usd,
       lead(a."day", 1, date_trunc('day', now()) + interval '1 day') over (partition by a."lp_name" order by a."day") as next_day
from changes_daily a left join qidao.view_lp_token_price b on a."day" = b."day" and a."lp_name" = b."name"
order by day
)
,days as (
SELECT generate_series('2021-5-2', date_trunc('day', now()), '1 day') as "day"
)
select b."day", a."lp_name", a."staked_lp_change",
       a."fee_in_lp_token", "fee_in_usd",
       a."cumulative_staked_lp_token",
       a."cumulative_staked_lp_value",
       a."cumulative_fee_in_lp_token",
       a."cumulative_fee_in_usd"
from changes_daily_ext a inner join days b
     on a."day" <= b."day" and a."next_day" > b."day" 
order by 1 desc
);

CREATE UNIQUE INDEX IF NOT EXISTS qidao_view_lp_pool_daily_status_idx ON qidao.view_lp_pool_daily_status (day, lp_name);

INSERT INTO cron.job(schedule, command)
VALUES ('15 */2 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_lp_pool_daily_status$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

COMMIT;