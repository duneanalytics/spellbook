CREATE MATERIALIZED VIEW tokemak.view_tokemak_outstanding_liabilities_daily
(
    "date", token_address, pricing_contract, symbol, is_dollar_stable, total_liability_qty,price_usd,price_eth,total_liability_value_usd, total_liability_value_eth
)
AS ( 

WITH calendar AS  
        (SELECT i::date as "date"
            ,tl.address
            ,tl.pricing_contract
            ,tl.is_dollar_stable
            ,tl.symbol
        FROM tokemak.view_tokemak_lookup_tokens tl
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
        WHERE tl.is_liability = true --AND NOT (i>'2022-05-10' AND (tl.address='\x7A75ec20249570c935Ec93403A2B840fBdAC63fd' OR tl.address='\x482258099de8de2d0bda84215864800ea7e6b03d')) order by "date" desc
 ) ,
minted as (
    SELECT 
        "date", address, pricing_contract, is_dollar_stable,symbol
        ,first_value(balance) OVER (PARTITION BY address, grpBalance ORDER BY "date") AS total_liability_qty
    FROM(
         SELECT c."date", c.address, c.pricing_contract, c.is_dollar_stable,c.symbol,r.balance,
            count(r.balance) OVER (PARTITION BY c.address ORDER BY c."date") AS grpBalance
                FROM calendar c 
                LEFT JOIN (
                 SELECT "date", address, pricing_contract, is_dollar_stable,symbol, SUM(amount) OVER (PARTITION BY address  ORDER BY "date") as balance FROM (
                     SELECT "date", address, pricing_contract,is_dollar_stable, symbol, Sum(amount) as amount from (
                        SELECT date_trunc('day',"evt_block_time") as "date", tl.address, tl.pricing_contract,tl.is_dollar_stable,  tl.symbol,
                        CASE WHEN tr."from" = '\x0000000000000000000000000000000000000000' THEN 
                                    value/10^tl.decimals
                             WHEN tr."to" = '\x0000000000000000000000000000000000000000'  THEN  
                                    -value/10^tl.decimals 
                             ELSE 
                                0
                             END as amount
                        FROM erc20."ERC20_evt_Transfer" tr
                        INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tr.contract_address = tl.address  AND  tl.is_liability = true 
                        WHERE tr."from" = '\x0000000000000000000000000000000000000000' OR tr."to" = '\x0000000000000000000000000000000000000000' 
                        AND NOT (tr."to" = tr."from") 
                        ) as t GROUP BY 1,2,3,4,5 
                    ) as tt ORDER BY "date" desc, symbol
                ) as r ON r."date" = c."date" and r.address = c.address
            ORDER BY "date" desc, symbol
    ) as result ORDER BY "date" desc, symbol
 ),

pools_and_wallets as (
 --liabilities minted but in our wallets
     SELECT 
        "date", address, pricing_contract, is_dollar_stable,symbol
        ,first_value(balance) OVER (PARTITION BY address, grpBalance ORDER BY "date") AS total_liability_qty
    FROM(
         SELECT c."date", c.address, c.pricing_contract, c.is_dollar_stable,c.symbol,r.balance,
            count(r.balance) OVER (PARTITION BY c.address ORDER BY c."date") AS grpBalance
            FROM calendar c 
            LEFT JOIN (
                SELECT "date", token_address, pricing_contract, is_dollar_stable, symbol, SUM(balance) as balance FROM 
                (
                 SELECT "date", token_address, pricing_contract, is_dollar_stable, symbol, sum(-balance) as balance 
                        FROM (
                            SELECT DISTINCT ON(date_trunc('day', "timestamp"), b.token_address)
                            date_trunc('day', "timestamp") as "date",
                            b.token_address,
                            tl.pricing_contract,
                            tl.is_dollar_stable,
                            tl.symbol as symbol,
                            tl.display_name,
                            b.amount_raw/10^tl.decimals as balance
                            FROM erc20."token_balances" b   --AND b.wallet_address='\x8b4334d4812c530574bd4f2763fcd22de94a969b' 
                            --order by "timestamp" desc
                            INNER JOIN tokemak."view_tokemak_addresses" ta ON ta.address = b.wallet_address
                            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON b.token_address = tl.address and tl.is_liability=true
                            ORDER BY "date" desc ,b.token_address, "timestamp" desc NULLS LAST
                        ) as t  GROUP BY 1,2,3,4,5 --order by "date" desc, symbol
                  UNION
                  SELECT "date", token_address, pricing_contract, is_dollar_stable, symbol, sum(-balance) as balance 
                                FROM (
                                SELECT DISTINCT ON(date_trunc('day', "timestamp"), b.token_address)
                                date_trunc('day', "timestamp") as "date",
                                b.token_address,
                                t.pricing_contract,
                                t.is_dollar_stable,
                                t.symbol as symbol,
                                t.display_name,
                                b.amount_raw/10^t.decimals as balance
                        FROM erc20."token_balances" b  
                        INNER JOIN tokemak."view_tokemak_lookup_tokens" t ON b.token_address = t.address AND is_liability=true 
                        WHERE  EXISTS (SELECT address FROM tokemak.view_tokemak_lookup_tokens tl WHERE  b.wallet_address = tl.address and tl.is_pool=true)
                        ORDER BY  "date" desc ,b.token_address, "timestamp" desc NULLS LAST
                        ) as t GROUP BY 1,2,3,4,5
                ) as tt 
                GROUP BY 1,2,3,4,5 ORDER BY "date" desc, symbol
            ) as r ON r."date" = c."date" AND r."token_address"=c."address"
        ) as result ORDER BY "date" desc, symbol
 )

SELECT m."date", m.address, m.pricing_contract, m.symbol,m.is_dollar_stable, COALESCE(m.total_liability_qty,0) + COALESCE(wp.total_liability_qty,0) as total_liability_qty, 
COALESCE(p.price_usd, 0) as price,
COALESCE(p.price_eth, 0) as price_eth,
(COALESCE(m.total_liability_qty,0) + COALESCE(wp.total_liability_qty,0)) * COALESCE(p.price_usd, 0) as total_liability_value_usd,
(COALESCE(m.total_liability_qty,0) + COALESCE(wp.total_liability_qty,0)) * COALESCE(p.price_eth, 0) as total_liability_value_eth  
FROM minted m LEFT JOIN pools_and_wallets wp on wp."date" = m."date" and wp.address = m.address
LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" p ON m.address = p.contract_address and m."date" = p."date"
ORDER BY m."date" desc, m.symbol
    
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_outstanding_liabilities_daily (
   "date",
   token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('9 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_outstanding_liabilities_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;