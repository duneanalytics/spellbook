CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.treasury_rfv;
create materialized view olympus.treasury_rfv as 

with time as
(
SELECT
generate_series('2021-03-23 00:00', NOW(), '1 day'::interval) as Date
),

treasury_dai AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D'
    
UNION ALL --aDAI in allocator
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x028171bca77440897b824ca71d1c56cac55b68a3' -- aDAI contract address
    and e."from" = '\x0e1177e47151be72e5992e0975000e73ab5fd9d4'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x028171bca77440897b824ca71d1c56cac55b68a3' -- aDAI contract address
    and e."to" = '\x0e1177e47151be72e5992e0975000e73ab5fd9d4'
    
),

final_treasury_dai as
(
SELECT
date_trunc('day',date) as date,
sum(sum(treasury_dai_supply)) over (order by date_trunc('day',date)) as treasury_dai
FROM 
(
SELECT Date, treasury_dai_supply as treasury_dai_supply FROM treasury_dai UNION ALL
SELECT Date, 0 as treasury_dai_supply FROM time
) t
GROUP BY 1
),

lp_dai AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as lp_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as lp_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    
),

final_lp_dai as
(
SELECT
date_trunc('day',date) as date,
sum(sum(lp_dai_supply)) over (order by date_trunc('day',date)) as lp_dai
FROM 
(
SELECT Date, lp_dai_supply as lp_dai_supply FROM lp_dai UNION ALL
SELECT Date, 0 as lp_dai_supply FROM time
) t
GROUP BY 1
),

lp_ohm AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e9 as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."from" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e9 as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."to" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    
),

final_lp_ohm as
(
SELECT
date_trunc('day',date) as date,
sum(sum(lp_ohm_supply)) over (order by date_trunc('day',date)) as lp_ohm
FROM 
(
SELECT Date, lp_ohm_supply as lp_ohm_supply FROM lp_ohm UNION ALL
SELECT Date, 0 as lp_ohm_supply FROM time
) t
GROUP BY 1
),

slp AS
(
    SELECT
    evt_block_time as Date,
    e.value/1e18 as slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."to" = '\x0000000000000000000000000000000000000000'
    
),

final_slp as
(
SELECT
date_trunc('day',date) as date,
sum(sum(slp_supply)) over (order by date_trunc('day',date)) as slp_supply
FROM 
(
SELECT Date, slp.slp_supply as slp_supply FROM slp UNION ALL
SELECT Date, 0 as slp_supply FROM time
) t
GROUP BY 1
),

treasury_slp AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- OHM contract address
    and e."from" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D '
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- OHM contract address
    and e."to" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D '
    
UNION ALL --treasury V2
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- OHM contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    and e."to" != '\x0316508a1b5abf1CAe42912Dc2C8B9774b682fFC' --ignore deployer transactions
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- OHM contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    and e."from" != '\x0316508a1b5abf1CAe42912Dc2C8B9774b682fFC' --ignore deployer transactions
    
UNION ALL ---ONSEN
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and (e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8' and e."to" = '\xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd')
    
UNION ALL
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and (e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8' and e."from" = '\xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd')
    
),

final_treasury_slp as
(
SELECT
date_trunc('day',date) as date,
sum(sum(treasury_slp_supply)) over (order by date_trunc('day',date)) as slp_treasury
FROM 
(
SELECT Date, treasury_slp_supply as treasury_slp_supply FROM treasury_slp UNION ALL
SELECT Date, 0 as treasury_slp_supply FROM time
) t
GROUP BY 1
),

--- FRAX-OHM LP PAIR

lp_frax AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as lp_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- frax contract address
    and e."from" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as lp_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- frax contract address
    and e."to" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    
),

final_lp_frax as
(
SELECT
date_trunc('day',date) as date,
sum(sum(lp_frax_supply)) over (order by date_trunc('day',date)) as lp_frax
FROM 
(
SELECT Date, lp_frax_supply as lp_frax_supply FROM lp_frax UNION ALL
SELECT Date, 0 as lp_frax_supply FROM time
) t
GROUP BY 1
),

f_lp_ohm AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e9 as f_lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- ohm contract address
    and e."from" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e9 as f_lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- ohm contract address
    and e."to" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    
),

final_f_lp_ohm as
(
SELECT
date_trunc('day',date) as date,
sum(sum(f_lp_ohm_supply)) over (order by date_trunc('day',date)) as f_lp_ohm
FROM 
(
SELECT Date, f_lp_ohm_supply as f_lp_ohm_supply FROM f_lp_ohm UNION ALL
SELECT Date, 0 as f_lp_ohm_supply FROM time
) t
GROUP BY 1
),

univ2 AS
(
    SELECT
    evt_block_time as Date,
    e.value/1e18 as supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- univ2 contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- univ2 contract address
    and e."to" = '\x0000000000000000000000000000000000000000'
    
),

final_univ2 as
(
SELECT
date_trunc('day',date) as date,
sum(sum(supply)) over (order by date_trunc('day',date)) as univ2_supply
FROM 
(
SELECT Date, univ2.supply as supply FROM univ2 UNION ALL
SELECT Date, 0 as supply FROM time
) t
GROUP BY 1
),

treasury_univ2 AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_univ2_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- ohm contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_univ2_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- ohm contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    
),

final_treasury_univ2 as
(
SELECT
date_trunc('day',date) as date,
sum(sum(treasury_univ2_supply)) over (order by date_trunc('day',date)) as treasury_univ2
FROM 
(
SELECT Date, treasury_univ2_supply as treasury_univ2_supply FROM treasury_univ2 UNION ALL
SELECT Date, 0 as treasury_univ2_supply FROM time
) t
GROUP BY 1
),

treasury_frax AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    
UNION ALL --convex allocator
  SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."from" = '\x3dF5A355457dB3A4B5C744B8623A7721BF56dF78' and e."to" != '\xa79828df1850e8a3a3064576f380d90aecdd3359'
    
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."to" = '\x3dF5A355457dB3A4B5C744B8623A7721BF56dF78' and e."from" != '\xa79828df1850e8a3a3064576f380d90aecdd3359'
    
),

final_treasury_frax as
(
SELECT
date_trunc('day',date) as date,
sum(sum(treasury_frax_supply)) over (order by date_trunc('day',date)) as treasury_frax
FROM 
(
SELECT Date, treasury_frax_supply as treasury_frax_supply FROM treasury_frax UNION ALL
SELECT Date, 0 as treasury_frax_supply FROM time
) t
GROUP BY 1
)

select time."date", 
(treasury_dai + (slp_treasury/slp_supply)*(2*sqrt(lp_dai * lp_ohm)) + treasury_frax + (treasury_univ2 / coalesce(NULLIF(univ2_supply,0),1) )*(2*sqrt(lp_frax * f_lp_ohm))) as treasury_rfv,
treasury_dai, lp_dai, lp_ohm, slp_treasury, slp_supply, treasury_frax, lp_frax, f_lp_ohm, treasury_univ2, univ2_supply
from time
left join final_lp_dai on final_lp_dai.date = time.date
left join final_lp_ohm on final_lp_ohm."date" = time.date
left join final_treasury_dai on final_treasury_dai."date" = time.date
left join final_slp on final_slp."date" = time.date
left join final_treasury_slp on final_treasury_slp."date" = time.date
left join final_lp_frax on final_lp_frax."date" = time.date
left join final_f_lp_ohm on final_f_lp_ohm."date" = time.date
left join final_univ2 on final_univ2."date" = time.date
left join final_treasury_univ2 on final_treasury_univ2."date" = time.date
left join final_treasury_frax on final_treasury_frax."date" = time.date
order by 1 desc


; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.treasury_rfv ("date", treasury_rfv, treasury_dai, lp_dai, lp_ohm, slp_treasury, slp_supply, treasury_frax, lp_frax, f_lp_ohm, treasury_univ2, univ2_supply);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 59 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.treasury_rfv$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
