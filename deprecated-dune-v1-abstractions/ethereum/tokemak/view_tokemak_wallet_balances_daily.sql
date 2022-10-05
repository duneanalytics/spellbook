CREATE MATERIALIZED VIEW tokemak.view_tokemak_wallet_balances_daily
(
    "date", source_name, wallet_address, token_address, symbol, display_name, tokemak_qty
) AS (
WITH calendar AS  
        (SELECT i::date as "date"
            ,s as source
            ,a.address as wallet_address
            ,tl.address
            ,tl.symbol
            ,tl.display_name
        FROM tokemak."view_tokemak_lookup_tokens" tl
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
        CROSS JOIN generate_series(0,4,1) tt(s) 
        CROSS JOIN (SELECT address from tokemak."view_tokemak_addresses") as a 
        --WHERE NOT (i>'2022-05-10' AND (tl.address='\xa47c8bf37f92aBed4A126BDA807A7b7498661acD' OR tl.address='\xa693b19d2931d498c5b318df961919bb4aee87a5' OR tl.address = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'))--remove UST tokens and pools
 ) ,
 result AS (
    SELECT "date", source, wallet_address, token_address, symbol, display_name, sum(balance)  as balance FROM (
        SELECT "date", source, wallet_address, token_address, symbol, display_name, sum(balance) as balance --OVER (PARTITION BY source, symbol ORDER BY "date") as balance 
            FROM (
            SELECT DISTINCT ON(date_trunc('day', "timestamp"), b.wallet_address, b.token_address)
            date_trunc('day', "timestamp") as "date",
            CASE WHEN starts_with(tl.display_name , 'Curve.fi') THEN 1 
            WHEN starts_with(tl.symbol, 'SUSHI-') THEN 3
            WHEN starts_with(tl.symbol, 'UNI-V2-') THEN 4
            ELSE 0 END as source,
            b.wallet_address,
            b.token_address,
            tl.symbol as symbol,
            tl.display_name,
            b.amount_raw/10^tl.decimals as balance
            FROM erc20."token_balances" b   --AND b.wallet_address='\x8b4334d4812c530574bd4f2763fcd22de94a969b' 
            --order by "timestamp" desc
            INNER JOIN tokemak."view_tokemak_addresses" ta ON ta.address = b.wallet_address
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON b.token_address = tl.address  
            -- WHERE NOT (date_trunc('day', "timestamp")::date >'2022-05-08' AND (tl.address='\xa47c8bf37f92aBed4A126BDA807A7b7498661acD' OR tl.address='\xa693b19d2931d498c5b318df961919bb4aee87a5' OR tl.address = '\xCEAF7747579696A2F0bb206a14210e3c9e6fB269'))--remove UST tokens and pools
            ORDER BY "date" desc , b.wallet_address, b.token_address, "timestamp" desc NULLS LAST
            ) as t  GROUP BY 1,2,3,4,5,6 
       -- ORDER BY "date" desc, source, symbol
        UNION
        --ETHER
        SELECT "date", source,wallet_address, token_address, symbol, display_name, SUM(balance) OVER (PARTITION BY wallet_address,symbol ORDER BY "date") as balance FROM (
                SELECT
                date_trunc('day', "block_time") as "date"
                ,0 as source, --0 mean undefined which is our wallets
                '\x8b4334d4812c530574bd4f2763fcd22de94a969b'::bytea as wallet_address,
                '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                 tl.symbol as symbol,
                tl.display_name,
                SUM(CASE WHEN ("to" = '\x8b4334d4812c530574bd4f2763fcd22de94a969b')
                    THEN value/10^tl.decimals 
                    ELSE -value/10^tl.decimals  END) as balance 
                FROM ethereum.traces 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON  tl.address ='\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
                WHERE ("to" = '\x8b4334d4812c530574bd4f2763fcd22de94a969b' OR "from" = '\x8b4334d4812c530574bd4f2763fcd22de94a969b') 
                AND NOT ("to" = "from")
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null) --order by "date" desc
                GROUP BY 1,2,3,4,5,6 --ORDER BY"date" desc
            UNION
                SELECT
                date_trunc('day', "block_time") as "date"
                ,0 as source, --0 mean undefined which is our wallets
                '\xa86e412109f77c45a3bc1c5870b880492fb86a14'::bytea as wallet_address,
                '\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address,
                 tl.symbol as symbol,
                tl.display_name,
                SUM(CASE WHEN ("to" = '\xa86e412109f77c45a3bc1c5870b880492fb86a14')
                    THEN value/10^tl.decimals 
                    ELSE -value/10^tl.decimals  END) as balance 
                FROM ethereum.traces 
                INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON  tl.address ='\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'
                WHERE ("to" = '\xa86e412109f77c45a3bc1c5870b880492fb86a14' OR "from" = '\xa86e412109f77c45a3bc1c5870b880492fb86a14') 
                AND NOT ("to" = "from")
                AND success
                AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null) --order by "date" desc
                GROUP BY 1,2,3,4,5,6 --ORDER BY"date" desc
                ) as t 
        UNION
        --Masterchef v1
        SELECT "date", 3 as source,wallet_address, contract_address as token_address, symbol,display_name, Sum(amount) OVER (PARTITION BY wallet_address,symbol  ORDER BY "date") as balance from (
            SELECT date_trunc('day', d."evt_block_time") as "date",t."from" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) as Amount FROM sushi."MasterChef_evt_Deposit" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address  AND t."to" = '\xc2edad668740f1aa35e4d8f227fb8e17dca888cd'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address 
            GROUP BY "date",t."from",t.contract_address,tl.symbol,tl.display_name
            UNION
            SELECT date_trunc('day', d."evt_block_time") as "date",t."to" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) *-1 as Amount FROM sushi."MasterChef_evt_Withdraw" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address  AND t."from" = '\xc2edad668740f1aa35e4d8f227fb8e17dca888cd'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address   
            GROUP BY "date",t."to",t.contract_address,tl.symbol,tl.display_name) as t
        --GROUP BY "date", source, t.contract_address,symbol,display_name 
        --order by "date" desc, symbol
        UNION
        --Masterchefv2 
        SELECT "date", 3 as source,wallet_address, contract_address as token_address, symbol,display_name, Sum(amount) OVER (PARTITION BY wallet_address,symbol  ORDER BY "date") as balance from (
            SELECT date_trunc('day', d."evt_block_time") as "date",t."from" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) as Amount FROM sushi."MasterChefV2_evt_Deposit" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address  AND t."to" = '\xef0881ec094552b2e128cf945ef17a6752b4ec5d'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address 
            GROUP BY "date",t."from",t.contract_address,tl.symbol,tl.display_name
            UNION
            SELECT date_trunc('day', d."evt_block_time") as "date",t."to" as wallet_address, t.contract_address,tl.symbol as symbol,tl.display_name, SUM(t.value/10^tl.decimals) *-1 as Amount FROM sushi."MasterChefV2_evt_Withdraw" d 
            INNER JOIN erc20."ERC20_evt_Transfer" t on t.evt_tx_hash = d.evt_tx_hash
            INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address  AND t."from" = '\xef0881ec094552b2e128cf945ef17a6752b4ec5d'
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON t.contract_address = tl.address   
            GROUP BY "date",t."to",t.contract_address,tl.symbol,tl.display_name) as t
        --GROUP BY source, contract_address,symbol,display_name
        --order by "date" desc, symbol
        UNION
        --CONVEX
        SELECT "date", source,wallet_address, token_address, symbol,display_name, sum(qty) OVER (PARTITION BY wallet_address,symbol ORDER BY "date")as balance 
        FROM (
                SELECT "date", 2 as source,wallet_address, contract_address as token_address, symbol,display_name, sum(qty) as qty FROM (
                    SELECT date_trunc('day', t."evt_block_time") as "date",t."to" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM((value/10^tl.decimals)*-1) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address and t."from"='\xF403C135812408BFbE8713b5A23a04b3D48AAE31'
                    GROUP BY 1,2,3,4,5
                    UNION 
                    SELECT  date_trunc('day', t."evt_block_time") as "date",t."from" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM(value/10^tl.decimals) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address and t."to"='\x989aeb4d175e16225e39e87d0d97a3360524ad80'
                    GROUP BY 1,2,3,4,5
                    UNION
                    SELECT  date_trunc('day', t."evt_block_time") as "date",t."from" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM(value/10^tl.decimals) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."from" = a.address and t."to"='\x72a19342e8f1838460ebfccef09f6585e32db86e' --voting escrow deposit
                    GROUP BY 1,2,3,4,5
                    UNION
                    SELECT  date_trunc('day', t."evt_block_time") as "date",t."from" as wallet_address,contract_address,tl.symbol,tl.display_name, SUM(value/10^tl.decimals*-1) as qty 
                    FROM erc20."ERC20_evt_Transfer" t
                    INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = t.contract_address
                    INNER JOIN tokemak."view_tokemak_addresses" a ON t."to" = a.address and t."from"='\x72a19342e8f1838460ebfccef09f6585e32db86e' --voting escrow withdrawal??  not sure about this as we have never withdrawn.  NEED TO VERIFY
                    GROUP BY 1,2,3,4,5
                )as t GROUP BY 1,2,3,4,5,6
            )as t --GROUP BY source, contract_address, symbol, display_name 
            ORDER BY "date" desc, symbol
        ) as t
        GROUP BY 1,2,3,4,5,6 
 --       ORDER BY "date" desc, source, symbol 
    ),
   -- SELECT * FROM result order  by "date" desc, symbol
    
temp_table AS ( 
        SELECT 
            c."date"
            ,c.source
            ,c.wallet_address
            , c.address
            , c.symbol
            , c.display_name
            , r.balance
            , count(balance) OVER (PARTITION BY c.source,c.wallet_address, c.address ORDER BY c."date") AS grpBalance
        FROM calendar c 
        LEFT JOIN result r on c."date"=r."date"  and c.address= r.token_address and  c.source=r.source AND c.wallet_address = r.wallet_address),
--SELECT * FROM temp_table WHERE balance >0 order by "date"desc, symbol 
  res_temp AS(    
    SELECT 
        "date"::date
        ,source
        ,wallet_address
        ,address
        ,symbol
        ,display_name
        ,first_value(balance) OVER (PARTITION BY source,wallet_address, address, grpBalance ORDER BY "date") AS tokemak_qty
    FROM  temp_table 
    order by "date" desc, source, symbol)
    
SELECT "date", s.source_name, wallet_address, address, symbol, display_name, tokemak_qty 
FROM res_temp t 
INNER JOIN tokemak."view_tokemak_lookup_sources" s on s.id = t.source
WHERE tokemak_qty >0 
ORDER BY "date" desc, source, wallet_address,symbol
);


CREATE UNIQUE INDEX ON tokemak.view_tokemak_wallet_balances_daily (
   "date",
   source_name,
   wallet_address,
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('13 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_wallet_balances_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
