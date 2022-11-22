DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_deployed_asset_balances_daily CASCADE
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_deployed_asset_balances_daily (
    "date"
    ,source_name
    ,pool_address
    ,pool_symbol
    ,total_lp_supply
    ,tokemak_lp_wallet_qty
    ,tokemak_lp_ownership_pct
    ,token_address
    ,token_symbol
    ,pool_reserve_qty
    ,tokemak_pool_reserve_qty  
    ,price_usd
    ,value_usd
    ,price_eth
    ,value_eth
)
AS
( 
   WITH combined as (
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_uniswap_pool_stats_daily"
        UNION
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_sushiswap_pool_stats_daily" 
        UNION
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_curve_pool_stats_daily"
        UNION
        SELECT source
            ,"date"
            ,pool_address
            ,pool_symbol
            ,token_address
            ,token_symbol
            ,total_lp_supply
            ,reserve FROM tokemak."view_tokemak_convex_pool_stats_daily" order by "date" desc, pool_symbol, token_symbol
        ),
base as (        
    SELECT 
        t."date"
        ,ls.source_name
        ,t.pool_address
        ,t.pool_symbol
        ,t.total_lp_supply
        ,wb.tokemak_qty as tokemak_lp_wallet_qty
        ,(wb.tokemak_qty/t.total_lp_supply) as tokemak_lp_ownership_pct
        ,t.token_address
        ,t.token_symbol
        ,t.reserve as pool_reserve_qty
        --,wb1.tokemak_qty as tokemak_reserve_wallet_qty
        ,(t.reserve * (wb.tokemak_qty/t.total_lp_supply)) as tokemak_pool_reserve_qty        
        FROM combined as t  
            INNER JOIN tokemak."view_tokemak_lookup_sources" ls on ls.id = t.source
            LEFT JOIN 
            (SELECT  "date", source_name, token_address, symbol, display_name, SUM(tokemak_qty) as tokemak_qty
            FROM tokemak."view_tokemak_wallet_balances_daily" 
            GROUP BY 1,2,3,4,5
            ORDER BY "date" desc, source_name, symbol) wb 
            ON wb."date" = t."date" AND t.pool_address = wb.token_address AND wb.source_name = ls.source_name
            ORDER BY t."date" desc, source_name asc, pool_symbol asc, token_symbol asc 

)
--SELECT * FROM base
,
pool_balances as (
    SELECT "date"
        ,source_name
        ,pool_address
        ,pool_symbol
        ,total_lp_supply
        ,tokemak_lp_wallet_qty
        ,tokemak_lp_ownership_pct
        ,token_address
        ,token_symbol
        ,pool_reserve_qty
        ,tokemak_pool_reserve_qty    FROM base 
        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = token_address AND tl.is_liability = false --remove liabilities
        order by "date" desc, source_name, pool_symbol 
        )

 , temp_balances AS(
 
    SELECT "date", source_name, token_address, bpool_address, btoken_symbol as token_symbol, sum(tokemak_qty) as tokemak_qty
    FROM (SELECT b."date",b.source_name,b.pool_address as bpool_address, b.pool_symbol as bpool_symbol,t.pool_address as tpool_address, 
        t.pool_symbol as tpool_symbol, b.token_address,t.token_symbol as ttoken_symbol,b.token_symbol as btoken_symbol,
        (t.tokemak_pool_reserve_qty/b.total_lp_supply)*b.pool_reserve_qty as tokemak_qty
        FROM
        pool_balances  b INNER JOIN pool_balances t ON t.token_address = b.pool_address and t.source_name = b.source_name and b."date" = t."date") as t
        GROUP BY "date", source_name, token_address,bpool_address, btoken_symbol 
        ORDER BY "date" desc, source_name, token_address,bpool_address, btoken_symbol 
 )
--SELECT * FROM temp_balances
    SELECT 
    p."date"
        ,p.source_name
        ,p.pool_address
        ,p.pool_symbol
        ,p.total_lp_supply
        ,p.tokemak_lp_wallet_qty
        ,p.tokemak_lp_ownership_pct
        ,p.token_address
        ,p.token_symbol
        , p.pool_reserve_qty
        , COALESCE(p.tokemak_pool_reserve_qty ,0) + COALESCE(t.tokemak_qty, 0) as  tokemak_pool_reserve_qty
       , tp.price_usd
       , (COALESCE(p.tokemak_pool_reserve_qty ,0) + COALESCE(t.tokemak_qty, 0)) * COALESCE(tp.price_usd,0) as value_usd
       , tp.price_eth
      , (COALESCE(p.tokemak_pool_reserve_qty ,0) + COALESCE(t.tokemak_qty, 0)) * COALESCE(tp.price_eth,0) as value_eth
        FROM pool_balances p
        LEFT JOIN temp_balances t on t."date" = p."date" AND t."source_name" = p."source_name" AND p.token_address = t.token_address and p.pool_address = t.bpool_address
        LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" tp on tp."contract_address" = p."token_address" and tp."date" = p."date"
        ORDER BY p."date" desc, p."source_name", p."pool_symbol", p."token_symbol"

 );
 CREATE UNIQUE INDEX ON tokemak.view_tokemak_deployed_asset_balances_daily (
   "date",
   source_name,
   pool_address,
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('20 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_deployed_asset_balances_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;