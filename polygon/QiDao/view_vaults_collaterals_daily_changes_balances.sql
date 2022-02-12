BEGIN;
DROP VIEW IF EXISTS qidao.view_vaults_collaterals_daily_changes_balances CASCADE;

CREATE VIEW qidao.view_vaults_collaterals_daily_changes_balances AS(
with vaults_daily_changes as
(
  select date_trunc('day', "date") as "day", "collateral_contract",
         "collateral_symbol", "collateral_price_symbol",
         sum("change_in_collateral_amount") as "change_in_collateral_amount"
  from qidao.view_vaults_collaterals_changes
  group by 1,2,3,4
  order by 1
)
,vaults_daily_changes_balances as
(
  select "day", "collateral_contract", "collateral_symbol",
         "collateral_price_symbol", "change_in_collateral_amount",
         sum("change_in_collateral_amount") over (partition by "collateral_symbol" order by "day") as "collateral_balance"
  from vaults_daily_changes
  order by 1
)
,daily_last_prices_time as
(
  select date_trunc('day', "minute") as "day", "symbol",
         max("minute") as "last_time"
  from prices.usd
  where "minute" >= '2021-5-1 00:00' and "symbol" in
        (select "collateral_price_symbol" from qidao."view_vaults_collaterals_mapping")
  group by 1, 2
  order by 1
)
,vaults_daily_changes_balances_prices as
(
  select a."day", a."collateral_contract", a."collateral_symbol",
         a."change_in_collateral_amount", a."collateral_balance",
         c."price" as "collateral_price_usd"
  from vaults_daily_changes_balances a
       inner join daily_last_prices_time b
         on a."day" = b."day" and a."collateral_price_symbol" = b."symbol"
       inner join prices.usd c
         on b."last_time" = c."minute" and b."symbol" = c."symbol"
  order by 1
)
,vaults_daily_changes_balances_values as
(
  select "day", "collateral_symbol", "change_in_collateral_amount",
         "collateral_balance",
         "collateral_balance" * "collateral_price_usd" as  "collateral_balance_value_usd",
         "collateral_price_usd"
  from vaults_daily_changes_balances_prices
  order by 1
)
select * from vaults_daily_changes_balances_values order by 1
);

COMMIT;