CREATE MATERIALIZED VIEW tokemak.view_tokemak_reactor_balances_daily
(
    "date", token_address, token_symbol, token_display_name, reactor_name, reactor_address, is_deployable, reactor_qty--,reactor_gross_value_usd, reactor_gross_value_eth
) AS (

WITH calendar AS  
        (SELECT i::date as "date"
            ,r.reactor_address as reactor_address
            ,r.reactor_name ,r.is_deployable
            ,r.underlyer_address as address
            ,tl.symbol
            ,tl.display_name
            
        FROM tokemak."view_tokemak_lookup_reactors" r  
        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = r.underlyer_address
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
 ) , 

reactor_underlyer_balances as (
    SELECT  "date",token_address, symbol, display_name,reactor_name,reactor_address,is_deployable, sum(balance) as reactor_qty FROM (
        SELECT DISTINCT ON(date_trunc('day', "timestamp"),b.wallet_address, b.token_address)
        date_trunc('day', "timestamp") as "date",
        b.wallet_address,
        b.token_address,
        r.reactor_name,
        r.reactor_address,
        r.is_deployable,
        tl.symbol as symbol,
        tl.display_name,
        b.amount_raw/10^tl.decimals as balance
        FROM erc20."token_balances" b  
        INNER JOIN tokemak."view_tokemak_lookup_reactors" r ON r.reactor_address = b.wallet_address AND r.underlyer_address = b.token_address AND r.underlyer_address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'  -- only weth
        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = b.token_address
        ORDER BY "date" desc,b.wallet_address, b.token_address, "timestamp" desc NULLS LAST
        ) as t 
    GROUP BY "date",token_address, symbol, display_name,reactor_name, reactor_address, is_deployable
    
    UNION    --need to do this because weth is only close to being correct in the aggregated table "token_balances" but our other tokens are only correct from the evt transfer table
     select "date", underlyer_address,symbol,display_name, reactor_name, reactor_address,is_deployable, SUM(amount) OVER (PARTITION BY  reactor_address  ORDER BY "date") as reactor_qty FROM (
        select "date", t.underlyer_address,tl.symbol,tl.display_name, t.reactor_name, t.reactor_address,t.is_deployable,  SUM(amount/10^tl.decimals) as amount  from 
            ( 
                select date_trunc('day', "evt_block_time") as "date", r.reactor_name,r.reactor_address,r.underlyer_address,r.is_deployable, "to",
                    SUM(value) as amount
                from erc20."ERC20_evt_Transfer" t
                INNER JOIN tokemak."view_tokemak_lookup_reactors" r ON r.underlyer_address = t.contract_address and r.reactor_address = "to" AND r.underlyer_address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                WHERE NOT (t."to" = t."from")
                GROUP BY 1,2,3,4,5,6
                union
                select date_trunc('day', "evt_block_time") as "date", r.reactor_name,r.reactor_address,r.underlyer_address,r.is_deployable,"from",
                    SUM(-value) as amount
                from erc20."ERC20_evt_Transfer" t
                INNER JOIN tokemak."view_tokemak_lookup_reactors" r ON r.underlyer_address = t.contract_address and r.reactor_address = "from" AND r.underlyer_address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
                WHERE NOT (t."to" = t."from")
                GROUP BY 1,2,3,4,5,6
            ) t INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = underlyer_address and tl.symbol <> ''
           group by 1,2,3,4,5,6,7
     ) as t  order by "date" desc, reactor_name
) ,

temp as (

    SELECT  c."date", c.address as token_address, c.symbol as token_symbol
    , c.display_name as token_display_name
    , c.reactor_name
    , c.reactor_address
    , c.is_deployable
    , r.reactor_qty
    , count(r.reactor_qty) OVER (PARTITION BY c.reactor_address ORDER BY c."date") AS grpQty
    FROM calendar c 
    LEFT JOIN reactor_underlyer_balances r on c."date" = r."date" and c.address = r.token_address and c.reactor_address = r.reactor_address
    GROUP BY 1,2,3,4,5,6,7,8
    ORDER BY c."date" desc, reactor_name, c.symbol
    ),

  res_temp AS(    
    SELECT 
        "date"
        , token_address
        , token_symbol
        , token_display_name
        , reactor_name
        , reactor_address
        , is_deployable
        ,first_value(reactor_qty) OVER (PARTITION BY reactor_address, grpQty ORDER BY "date") AS reactor_qty
    FROM  temp
    order by "date" desc, reactor_name)

    SELECT  r."date", token_address, token_symbol, token_display_name, reactor_name, reactor_address, is_deployable, reactor_qty 
    FROM res_temp r
    WHERE reactor_qty <> 0 order by "date" desc, reactor_name
 
);


CREATE UNIQUE INDEX ON tokemak.view_tokemak_reactor_balances_daily (
   "date",
   token_address,
   reactor_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('10 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_reactor_balances_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
