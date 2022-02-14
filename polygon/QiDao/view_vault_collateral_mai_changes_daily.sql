BEGIN;
DROP MATERIALIZED VIEW IF EXISTS qidao.view_vault_collateral_mai_changes_daily;

CREATE MATERIALIZED VIEW qidao.view_vault_collateral_mai_changes_daily AS (
with daily_changes as
(
  select date_trunc('day', "block_time") as "day",
         "vault_contract_address", "collateral_contract_address",
         "collateral_symbol", "collateral_price_symbol",
         sum("change_in_collateral") as "change_in_collateral",
         sum("change_in_issued_mai") as "change_in_issued_mai",
         sum("fee_in_collateral") as "fee_in_collateral",
         sum("fee_in_usd") as "fee_in_usd"
  from qidao.view_vault_collateral_mai_changes
  group by 1, 2, 3, 4, 5
  order by 1
)
,daily_changes_ext as
(
  select *,
         sum("change_in_collateral") over (partition by "collateral_symbol" order by "day") as "cumulative_collateral",
         sum("change_in_issued_mai") over (partition by "collateral_symbol" order by "day") as "cumulative_issued_mai",
         sum("fee_in_collateral") over (partition by "collateral_symbol" order by "day") as "cumulativ_fee_in_collateral",
         sum("fee_in_usd") over (partition by "collateral_symbol" order by "day") as "cumulative_fee_in_usd"
  from daily_changes
)
,daily_last_price_time as
(
  select date_trunc('day', "minute") as "day", "symbol",
         max("minute") as "last_time"
  from prices.usd
  where "minute" >= '2021-5-1 00:00' and "symbol" in
        (select "collateral_price_symbol" from qidao.view_vault_collateral_mappings)
  group by 1, 2
  order by 1
),
daily_changes_ext_price as
(
  select a.*,
         a."change_in_collateral" * c."price" as "change_in_collateral_value_usd",
         a."cumulative_collateral" * c."price" as "cumulative_collateral_value_usd",
         c."price" as "collateral_price_usd"
  from daily_changes_ext a
       left join daily_last_price_time b
         on a."day" = b."day" and a."collateral_price_symbol" = b."symbol"
       left join prices.usd c
         on b."last_time" = c."minute" and b."symbol" = c."symbol"
)
select *,
      case when "cumulative_issued_mai" > 0 then "cumulative_collateral_value_usd"/"cumulative_issued_mai"
           else NULL
      end as "collateral_ratio"
from daily_changes_ext_price
);

INSERT INTO cron.job(schedule, command)
VALUES ('3 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY qidao.view_vault_collateral_mai_changes_daily$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
COMMIT;