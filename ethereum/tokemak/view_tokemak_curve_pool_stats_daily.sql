DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_curve_pool_stats_daily
;

CREATE MATERIALIZED VIEW tokemak."view_tokemak_curve_pool_stats_daily"
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
    
WITH   pools_and_constituents As 
    (SELECT  t."date",pool_address,pool_symbol,token_address,t.symbol as token_symbol,qty
                                                                     
    FROM(
        --3crv
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, 
        (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' OR "from" = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7' 
    UNION    
        --fraxUSDC
       SELECT "date", token_address,p.base_pool_symbol as pool_symbol,p.pool_token_address as pool_address, m.symbol, 
        (qty/10^m.decimals) as qty  FROM (
            SELECT "date",token_address,
            SUM(qty) OVER (PARTITION BY token_address ORDER BY "date")as qty
            FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                    contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' THEN value ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' OR "from" = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY "date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2' 
    UNION    
       --wormhole
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as "date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269' OR "from" = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269') AND contract_address <> '\xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48' --somehow $100 worth of USDC was put in this pool so we need to omit it
                AND NOT ("to" = "from") AND DATE_TRUNC('day', evt_block_time) < '2022-05-11'
                GROUP BY 1,2 
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'
       AND qty>0 
    UNION 
    --alUSD
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c' OR "from" = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt --order by "date" desc
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x43b4FdFD4Ff969587185cDB6f0BD875c5Fc83f8c'
       AND qty>0 
    UNION    
    --FRAX3CRV
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B' OR "from" = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B')
                AND contract_address <>'\x4eb8b4c65d8430647586cf44af4bf23ded2bb794' --need to omit anything that was airdropped into the pool
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" 
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xd632f22692FaC7611d2AA1C0D552930D43CAEd3B'
       AND qty>0  
    UNION
    --LUSD3CRV
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA' THEN value  ELSE value *-1  END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA' OR "from" = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" 
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xEd279fDD11cA84bEef15AF5D39BB4d4bEE23F0cA'
       AND qty>0  
    UNION   
        --ALCX
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74' OR "from" = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x9001a452d39A8710D27ED5c2E10431C13F5Fba74'
       AND qty>0 
    UNION    
        --TCR
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F' OR "from" = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x01FE650EF2f8e2982295489AE6aDc1413bF6011F'
       AND qty>0
    UNION    
        --SUSHI
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x0437ac6109e8A366A1F4816edF312A36952DB856' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x0437ac6109e8A366A1F4816edF312A36952DB856' OR "from" = '\x0437ac6109e8A366A1F4816edF312A36952DB856')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x0437ac6109e8A366A1F4816edF312A36952DB856'
       AND qty>0
    UNION    
        --FXS
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x961226B64AD373275130234145b96D100Dc0b655' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x961226B64AD373275130234145b96D100Dc0b655' OR "from" = '\x961226B64AD373275130234145b96D100Dc0b655')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x961226B64AD373275130234145b96D100Dc0b655'
       AND qty>0
    UNION    
        --FOX
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC' OR "from" = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xC250B22d15e43d95fBE27B12d98B6098f8493eaC'
       AND qty>0
    UNION
    --APW
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9' OR "from" = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\xCaf8703f8664731cEd11f63bB0570E53Ab4600A9'
       AND qty>0
    UNION    
        --SNX
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4' OR "from" = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x50B0D9171160d6EB8Aa39E090Da51E7e078E81c4'
       AND qty>0
    UNION
    --GAMMA
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA' OR "from" = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x9462F2b3C9bEeA8afc334Cdb1D1382B072e494eA'
       AND qty>0 
      UNION
    --WETH
        SELECT"date", token_address,p.symbol as pool_symbol,p.address as pool_address, m.symbol as token_symbol, (qty/10^m.decimals) as qty  FROM (
            SELECT"date",token_address,SUM(qty) OVER (PARTITION BY token_address ORDER BY"date")as qty FROM
            (
                SELECT
                    DATE_TRUNC('day', evt_block_time) as"date",
                     contract_address as token_address,
                    SUM(CASE WHEN "to" = '\x06d39e95977349431e3d800d49c63b4d472e10fb' THEN value ELSE value *-1 END) as qty 
                FROM erc20."ERC20_evt_Transfer" 
                WHERE ("to" = '\x06d39e95977349431e3d800d49c63b4d472e10fb' OR "from" = '\x06d39e95977349431e3d800d49c63b4d472e10fb')
                AND NOT ("to" = "from")
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_tokens" p WHERE p.address = '\x06d39e95977349431e3d800d49c63b4d472e10fb'
       AND qty>0 
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
                AND success
                AND NOT ("to" = "from")
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
                GROUP BY 1,2 --ORDER BY"date" desc
            ) as tt --order by "date" desc
       )as t 
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
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
       INNER JOIN tokemak."view_tokemak_lookup_tokens" m ON m.address = t.token_address
       CROSS JOIN tokemak."view_tokemak_lookup_metapools" p WHERE p.base_pool_address = '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'
       AND qty>0 
    )as t 
    --order by "date" desc, t.pool_symbol asc
    )
    
,  calendar AS  
    (SELECT DISTINCT  i::date as "date", pool_address,pool_symbol,token_address,token_symbol
        FROM pools_and_constituents
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i)
        --ORDER BY "date" desc, pool_symbol asc
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
  --order by "date" desc, pool_symbol 
  )
 
    SELECT 
        1 as source
        ,t."date"
        ,pool_address
        ,pool_symbol
        ,token_address
        ,token_symbol
        ,ts.total_supply as lp_total_supply
        ,first_value(qty) OVER (PARTITION BY pool_address, token_address, grpQty ORDER BY t."date") AS qty
        FROM  temp t
        INNER JOIN tokemak."view_tokemak_curve_convex_pool_total_supply" ts ON (t.pool_address = ts.address AND ts."date" = t."date")
        --ORDER BY "date" desc, pool_symbol asc
        
            
);


CREATE UNIQUE INDEX ON tokemak.view_tokemak_curve_pool_stats_daily (
   "date",
   pool_address,
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('5 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_curve_pool_stats_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;