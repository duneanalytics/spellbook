CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.bond_discount;
create materialized view olympus.bond_discount as 

with swap AS ( 
        SELECT
            sw."evt_block_time" AS minute,
            ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
            
        FROM sushi."Pair_evt_Swap" sw
        WHERE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c' -- liq pair OHM-DAI
        --AND sw.evt_block_time > current_date - interval '45 days'
        ), 

price as 
    (select swap."minute", 
    (a1_amt/a0_amt) as price
from swap
order by 1 desc),
    
bond_price as 
(select evt_block_time as minute, 
    deposit/1e18 as deposit, 
    payout/1e9 as payout, 
    expires, 
    "priceInUSD"/1e18 as bond_price, 
    contract_address, evt_tx_hash,
    evt_index, 
    evt_block_number
from olympus."OlympusBondDepository_evt_BondCreated"
--WHERE evt_block_time > current_date - interval '45 days'
),

discount as (
    SELECT minute as "date", 
        (select price from price where price.minute <= bond_price.minute order by bond_price.minute limit 1) as price,  
        bond_price, 
        ((select price from price where price.minute <= bond_price.minute order by bond_price.minute limit 1)-bond_price)/bond_price as discount, 
        contract_address
    FROM bond_price 
   order by 1 desc)
   
 select "date", price, bond_price, discount,
 AVG(discount) OVER(ORDER BY date ROWS BETWEEN 99 PRECEDING AND CURRENT ROW) AS "discount_90_ma",
 contract_address
 from discount


; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.bond_discount ("date", price, bond_price, discount,"discount_90_ma",contract_address);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 59 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.bond_discount$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;