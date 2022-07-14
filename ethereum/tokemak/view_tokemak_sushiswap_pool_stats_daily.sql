CREATE MATERIALIZED VIEW tokemak.view_tokemak_sushiswap_pool_stats_daily
(   source
    ,"date"
    ,pool_address
    ,pool_symbol
    ,token_address
    ,token_symbol
    ,total_lp_supply
    ,reserve
    --,cumulative_fees
) AS (
    WITH pairs AS(
            SELECT t.token_address, t.symbol as token_symbol,t.token_decimals, t.index,t.pool_address,tl.symbol as pool_symbol, tl.decimals as pool_decimals FROM(
                SELECT token0 as token_address, tl.symbol,tl.decimals as token_decimals, pair as pool_address, 1 as index FROM sushi."Factory_evt_PairCreated"
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token0
                UNION
                SELECT token1 as token_address, tl.symbol,tl.decimals as token_decimals, pair as pool_address, 2 as index FROM sushi."Factory_evt_PairCreated"
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token1
            ) as t INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = pool_address 
            --ORDER BY pool_symbol, token_symbol
        ),
    pools as (
        SELECT DISTINCT pool_address, pool_decimals FROM pairs
    )
    
    , calendar AS (
        SELECT c.*, p.token_address, p.token_symbol,p.index, p.token_decimals 
        FROM (SELECT i::date as "date"
            ,tl.address as pool_address
            ,tl.symbol as pool_symbol
            ,tl.decimals as pool_decimals
            FROM sushi."Factory_evt_PairCreated" pc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on pc.pair = tl.address
            CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
            WHERE tl.is_pool = true order by "date" desc) c
        INNER JOIN pairs p ON p.pool_address = c.pool_address
        --ORDER BY "date" desc, c.pool_symbol asc
    )
    ,supply AS
        (SELECT 
            "date"
            ,d.pool_address 
            ,SUM(transfer/10^d.pool_decimals) OVER (PARTITION BY d.pool_address ORDER BY "date") AS supply
        FROM (SELECT 
                    date_trunc('day', evt_block_time) AS "date"
                    ,t.pool_address 
                    ,t.pool_decimals
                    ,sum(value) AS transfer
                FROM (SELECT 
                        evt_block_time
                        ,p.pool_address
                        ,p.pool_decimals
                        ,CASE WHEN "from" = '\x0000000000000000000000000000000000000000' THEN value ELSE -value END as value
                    FROM sushi."Pair_evt_Transfer" t
                    INNER JOIN pools p ON p.pool_address = t.contract_address 
                    WHERE ("from" = '\x0000000000000000000000000000000000000000' OR  "to" = '\x0000000000000000000000000000000000000000')

                    ) AS t GROUP BY 1, 2, 3 --order by "date" desc
            ) AS d --order by "date" desc
        )

    ,reserves AS
        (SELECT c."date"
                ,c.pool_address
                ,c.pool_symbol
                ,c.pool_decimals
                ,c.token_address
                ,c.token_decimals
                ,c.token_symbol
                ,c.index
                ,latest_reserves[c.index+2]/10^c.token_decimals AS reserve
        FROM calendar c 
        LEFT JOIN
            (SELECT date_trunc('day', t.evt_block_time)::date as "date"
                ,tl.address as pool_address
                ,MAX(ARRAY[evt_block_number, evt_index, reserve0, reserve1]) AS latest_reserves
                FROM sushi."Pair_evt_Sync" t 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = t.contract_address 
                GROUP BY 1, 2 ORDER BY "date" desc) dr ON c.pool_address = dr.pool_address and dr."date" = c."date")

    /*,fees AS
        (SELECT 
            c."date"
            ,c.pool_address
            ,c.token_address
            ,c.token_decimals
            ,c.index
            ,CASE WHEN c.index = 1 THEN (0.003*sum("amount0In"/10^c.token_decimals)) ELSE (0.003*sum("amount1In"/10^c.token_decimals)) END AS token_fees 
        FROM calendar c LEFT JOIN
        sushi."Pair_evt_Swap" t  ON c.pool_address = t.contract_address and c."date" = t."evt_block_time"::date
        GROUP BY 1,2,3,4,5) */
        
    ,temp_table AS
        (SELECT DISTINCT
            c."date"
            ,c.pool_address
            ,c.pool_symbol
            ,c.token_address 
            ,c.token_symbol 
            ,r.reserve
            ,s.supply
            --,f.token_fees
            ,count(s.supply) OVER (PARTITION BY c.pool_address ORDER BY c."date") AS grpSupply
            ,count(r.reserve) OVER (PARTITION BY c.pool_address,c.token_address ORDER BY c."date") AS grpRes
        FROM calendar c 
        LEFT JOIN supply s ON s.pool_address = c.pool_address AND c."date" = s."date"
        INNER JOIN reserves r ON c."date" = r."date" AND r.token_address = c.token_address AND r.pool_address = c.pool_address
        --INNER JOIN fees f ON c."date"=f."date" AND f.token_address = c.token_address AND f.pool_address = c.pool_address
        --ORDER BY c."date" desc, pool_symbol asc, token_symbol asc
        )

    SELECT 
        3
        ,"date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,first_value(supply) OVER (PARTITION BY pool_address, grpSupply ORDER BY "date") AS supply
        ,first_value(reserve) OVER (PARTITION BY pool_address, token_address, grpRes ORDER BY "date") AS reserve
        --,sum(token_fees) OVER (PARTITION BY pool_address,token_address ORDER BY "date") AS cumulative_fees
            FROM  temp_table order by "date" desc, pool_symbol asc, token_symbol asc
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_sushiswap_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('7 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_sushiswap_pool_stats_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

