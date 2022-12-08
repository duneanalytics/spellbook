DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_convex_frax_pool_stats_daily
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_convex_frax_pool_stats_daily
(   
    source
    ,"date"
    ,pool_address
    ,pool_symbol
    ,token_address
    ,token_symbol
    ,total_lp_supply
    ,reserve
) AS (
    
WITH convex_frax_pools As 
    (
        SELECT contract_address as pool_address, symbol, sum(qty) as qty 
            FROM (
            SELECT contract_address,tl.symbol, (value/10^tl.decimals)*-1 as qty 
            FROM erc20."ERC20_evt_Transfer" t
            INNER JOIN tokemak.view_tokemak_lookup_tokens tl on tl.address = t.contract_address
            WHERE t."to"='\xA86e412109f77c45a3BC1c5870b880492Fb86A14' and t."from"='\xF403C135812408BFbE8713b5A23a04b3D48AAE31'
            AND NOT (t."to" = t."from")
            UNION
            SELECT contract_address,tl.symbol, value/10^tl.decimals as qty 
            FROM erc20."ERC20_evt_Transfer" t
            INNER JOIN tokemak.view_tokemak_lookup_tokens tl on tl.address = t.contract_address
            WHERE t."from"='\xA86e412109f77c45a3BC1c5870b880492Fb86A14' and t."to"='\x989aeb4d175e16225e39e87d0d97a3360524ad80'
            AND NOT (t."to" = t."from")
        )as t GROUP BY contract_address, symbol 
    ),
    pools_and_constituents AS (
        --fraxUSDC
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE "to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' OR "from" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'  
 )
 
,  calendar AS  
    (SELECT DISTINCT  i::date as "date", pool_address,pool_symbol,token_address,token_symbol
        FROM pools_and_constituents
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
        )

  , temp AS
  (
  SELECT c."date"
  ,c.pool_address
  ,c.pool_symbol
  ,c.token_address
  ,c.token_symbol
  ,pc.qty  
  ,count(pc.qty) OVER (PARTITION BY c.pool_address,c.token_address ORDER BY c."date") AS grpQty
  FROM calendar c 
  LEFT JOIN pools_and_constituents pc on pc."date" = c."date" AND pc.pool_address = c.pool_address AND pc.token_address = c.token_address 
  )

    SELECT 
        5 as source
        ,t."date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,ts.total_supply as lp_total_supply
        ,first_value(qty) OVER (PARTITION BY pool_address, token_address, grpQty ORDER BY t."date") AS qty
        FROM  temp t 
        INNER JOIN tokemak."view_tokemak_curve_convex_pool_total_supply" ts ON (t.pool_address = ts.address AND ts."date" = t."date")
        ORDER BY "date" desc, pool_symbol asc

        
            
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_convex_frax_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('9 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_convex_frax_pool_stats_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

