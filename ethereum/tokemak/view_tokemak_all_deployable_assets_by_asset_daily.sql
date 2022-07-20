CREATE MATERIALIZED VIEW tokemak.view_tokemak_all_deployable_assets_by_asset_daily
(
    "date", token_symbol,token_address, total_qty--,total_value_usd,total_value_eth
)
AS (
    SELECT "date", token_symbol, token_address, SUM(total_qty) as total_qty--, SUM(total_value_usd) as total_value_usd, SUM(total_value_eth) as total_value_eth 
    FROM(
        SELECT "date",
        token_symbol,token_address
       ,tokemak_pool_reserve_qty as total_qty
       --,value_usd as total_value_usd
       --,value_eth as total_value_eth
        FROM tokemak."view_tokemak_deployed_asset_balances_daily" 
        UNION
        SELECT "date",
         token_symbol,token_address
        ,reactor_qty AS total_qty
        --,reactor_gross_value_usd as total_value_usd
        --,reactor_gross_value_eth as total_value_eth
        FROM tokemak."view_tokemak_reactor_balances_daily" b 
        WHERE b.is_deployable = true 
    ) as t GROUP BY 1,2,3 ORDER BY "date" desc, token_symbol
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_all_deployable_assets_by_asset_daily (
   "date", token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('23 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_all_deployable_assets_by_asset_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;