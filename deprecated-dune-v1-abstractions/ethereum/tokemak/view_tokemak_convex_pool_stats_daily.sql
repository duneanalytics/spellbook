DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_convex_pool_stats_daily
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_convex_pool_stats_daily
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
    
WITH  convex_pools As 
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
     UNION
        --3crv
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE "to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' OR "from" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'
      UNION
      --ETH and stETH 
         SELECT"date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol,  (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', block_time) as"date",
                    '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                    SUM(CASE WHEN "to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022' THEN value ELSE value *-1 END) as qty 
                FROM ethereum.traces 
                WHERE ("to" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022' OR "from" = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022')
                AND NOT ("to" = "from")
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt --order by "date" desc
       )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
        UNION
        --stETH
        SELECT DATE_TRUNC('day', call_block_time) as"date",token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT DISTINCT ON (call_block_time::date) call_block_time
            , contract_address as token_address
            , output_0 as qty
                FROM lido."steth_call_balanceOf" where _account = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
                    --order by call_block_time::date desc, call_block_time desc
           )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
       --all other convex pools
    UNION 
       SELECT"date", token_address,p.symbol as pool_symbol, pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,pool_address,SUM(qty) OVER (PARTITION BY pool_address,token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                        cp.pool_address as pool_address,
                     t.contract_address as token_address,
                    SUM(CASE WHEN "to" = cp.pool_address THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" t INNER JOIN convex_pools cp on (t."to" = cp.pool_address OR t."from" = cp.pool_address) and cp.pool_address<>'\x06325440D014e39736583c165C2963BA99fAf14E' --omit the steETH pool because we get those quantities above
                AND NOT (cp.pool_address = '\xceaf7747579696a2f0bb206a14210e3c9e6fb269' AND t.contract_address = '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48')--somehow $100 worth of USDC was put in this pool so we need to omit it otherwise it looks like the ust 3crv pool also consists of a third instrument
                WHERE t.contract_address <>'\x4eb8b4c65d8430647586cf44af4bf23ded2bb794'   --need to omit anything that was airdropped into the pool 
                AND NOT (t."to" = t."from")
                GROUP BY 1,2,3 
            ) as tt 
      )as t 
       INNER JOIN tokemak.view_tokemak_lookup_tokens m ON m.address = t.token_address  
       CROSS JOIN tokemak.view_tokemak_lookup_tokens p WHERE p.address = t.pool_address
      AND qty>0  
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
        2 as source
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

CREATE UNIQUE INDEX ON tokemak.view_tokemak_convex_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('9 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_convex_pool_stats_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

