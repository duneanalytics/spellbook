CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.treasury_mv;
create materialized view olympus.treasury_mv as 

with time as
(
SELECT
generate_series('2021-03-23 00:00', NOW(), '1 day'::interval) as Date
),

swap AS ( 
        SELECT
            date_trunc('day', sw."evt_block_time") AS day,
            ("amount0In" + "amount0Out")/1e9 AS a0_amt, 
            ("amount1In" + "amount1Out")/1e18 AS a1_amt
            
        FROM sushi."Pair_evt_Swap" sw
        WHERE contract_address = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c' -- liq pair address I am searching the price for
        AND sw.evt_block_time >= '2021-03-22'
        ), 
        
        a1_prcs AS (
        SELECT avg(price) a1_prc, date_trunc('day', minute) AS day
        FROM prices.usd
        WHERE minute >= '2021-03-22'
        and contract_address ='\x6b175474e89094c44da98b954eedeac495271d0f'
        group by 2
        ),

   final_prices as (SELECT
        (AVG((a1_amt/a0_amt)*a1_prc)) AS price,
        a1_prcs."day" AS day
    FROM swap 
    JOIN a1_prcs ON swap."day" = a1_prcs."day"
    GROUP BY 2
    ORDER BY "day" DESC
),

treasury_dai AS
(
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D'
    GROUP BY 1
UNION ALL --treasury aDAI
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x028171bca77440897b824ca71d1c56cac55b68a3' -- aDAI contract address
    and e."from" = '\x0e1177e47151be72e5992e0975000e73ab5fd9d4'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x028171bca77440897b824ca71d1c56cac55b68a3' -- aDAI contract address
    and e."to" = '\x0e1177e47151be72e5992e0975000e73ab5fd9d4'
    GROUP BY 1
),

final_treasury_dai as
(
SELECT
Date,
sum(sum(treasury_dai_supply)) over (order by Date) as treasury_dai
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as lp_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as lp_dai_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
),

final_lp_dai as
(
SELECT
Date,
sum(sum(lp_dai_supply)) over (order by Date) as lp_dai
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e9), 0) as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."from" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e9), 0) as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."to" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
),

final_lp_ohm as
(
SELECT
Date,
sum(sum(lp_ohm_supply)) over (order by Date) as lp_ohm
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- OHM contract address
    and e."to" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
),

final_slp as
(
SELECT
Date,
sum(sum(slp_supply)) over (order by Date) as slp_supply
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."from" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D '
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."to" = '\x886CE997aa9ee4F8c2282E182aB72A705762399D '
    GROUP BY 1
UNION ALL --treasury V2
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    and e."to" != '\x0316508a1b5abf1CAe42912Dc2C8B9774b682fFC' --ignore deployer transactions
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    and e."from" != '\x0316508a1b5abf1CAe42912Dc2C8B9774b682fFC' --ignore deployer transactions
    GROUP BY 1
UNION ALL ---ONSEN
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and (e."from" = '\x245cc372c84b3645bf0ffe6538620b04a217988b' and e."to" = '\xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd')
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_slp_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and (e."to" = '\x245cc372c84b3645bf0ffe6538620b04a217988b' and e."from" = '\xc2EdaD668740f1aA35E4D8f227fB8E17dcA888Cd')
    GROUP BY 1
),

final_treasury_slp as
(
SELECT
Date,
sum(sum(treasury_slp_supply)) over (order by Date) as slp_treasury
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as lp_frax_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- frax contract address
    and e."from" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as lp_frax_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- frax contract address
    and e."to" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    GROUP BY 1
),

final_lp_frax as
(
SELECT
Date,
sum(sum(lp_frax_supply)) over (order by Date) as lp_frax
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e9), 0) as f_lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- ohm contract address
    and e."from" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e9), 0) as f_lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- ohm contract address
    and e."to" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    GROUP BY 1
),

final_f_lp_ohm as
(
SELECT
Date,
sum(sum(f_lp_ohm_supply)) over (order by Date) as f_lp_ohm
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- univ2 contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- univ2 contract address
    and e."to" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
),

final_univ2 as
(
SELECT
Date,
sum(sum(supply)) over (order by Date) as univ2_supply
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_univ2_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- ohm contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_univ2_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877' -- ohm contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
),

final_treasury_univ2 as
(
SELECT
Date,
sum(sum(treasury_univ2_supply)) over (order by Date) as treasury_univ2
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
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL --convex allocator
  SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."from" = '\x3dF5A355457dB3A4B5C744B8623A7721BF56dF78' and e."to" != '\xa79828df1850e8a3a3064576f380d90aecdd3359'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_frax_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."to" = '\x3dF5A355457dB3A4B5C744B8623A7721BF56dF78' and e."from" != '\xa79828df1850e8a3a3064576f380d90aecdd3359'
    GROUP BY 1
),

final_treasury_frax as
(
SELECT
Date,
sum(sum(treasury_frax_supply)) over (order by Date) as treasury_frax
FROM 
(
SELECT Date, treasury_frax_supply as treasury_frax_supply FROM treasury_frax UNION ALL
SELECT Date, 0 as treasury_frax_supply FROM time
) t
GROUP BY 1
),

treasury_xsushi as (
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_xsushi_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x8798249c2e607446efb7ad49ec89dd1865ff4272' -- xSUSHI contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_xsushi_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\x8798249c2e607446efb7ad49ec89dd1865ff4272' -- xSUSHI contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
),

final_treasury_xsushi as
(
SELECT
Date,
sum(sum(treasury_xsushi_supply)) over (order by Date) as treasury_xsushi
FROM 
(
SELECT Date, treasury_xsushi_supply as treasury_xsushi_supply FROM treasury_xsushi UNION ALL
SELECT Date, 0 as treasury_xsushi_supply FROM time
) t
GROUP BY 1
),

-----XSUSHI PRICE QUERY
dex_trades AS (
        SELECT 
            token_a_address as contract_address, 
            usd_amount/token_a_amount as price,
            block_time
        FROM dex.trades
        WHERE 1=1
        AND usd_amount  > 0
        AND category = 'DEX'
        AND token_a_amount > 0
        AND token_a_address = '\x8798249c2E607446EfB7Ad49eC89dD1865Ff4272'
        UNION ALL 
        
        SELECT 
            token_b_address as contract_address, 
            usd_amount/token_b_amount as price,
            block_time
        FROM dex.trades
        WHERE 1=1
        AND usd_amount  > 0
        AND category = 'DEX'
        AND token_b_amount > 0
        AND token_b_address = '\x8798249c2E607446EfB7Ad49eC89dD1865Ff4272'
        
    ),

rawdata as (
    SELECT 
        date_trunc('day', block_time) as day,
        d.contract_address,
        e.symbol as asset,
        (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price)) AS price,
        count(1) AS sample_size
    FROM dex_trades d
    left join erc20.tokens e on e.contract_address = d.contract_address
    GROUP BY 1, 2, 3
    ),
leaddata as 
    (
    SELECT
    day,
    contract_address,
    asset,
    price,
    sample_size,
    lead(DAY, 1, now() ) OVER (PARTITION BY contract_address ORDER BY day asc) AS next_day
    from rawdata
    where sample_size > 3
    ),
days AS
    (
    SELECT
    generate_series('2020-01-01'::TIMESTAMP, date_trunc('day', NOW()), '1 day') AS DAY
    ),
xsushi_price as (SELECT
    d.day as day,
    contract_address,
    asset,
    price as xsushi_price,
    sample_size
    from leaddata b
    INNER JOIN days d ON b.day <= d.day
    AND d.day < b.next_day -- Yields an observation for every day after the first transfer until the next day with transfer
    ),
    
treasury_weth as (
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as treasury_weth_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- weth contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', evt_block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as treasury_weth_supply
    FROM erc20."ERC20_evt_Transfer" e

    WHERE "contract_address" = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2' -- weth contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
    GROUP BY 1
),

final_treasury_weth as
(
SELECT
Date,
sum(sum(treasury_weth_supply)) over (order by Date) as treasury_weth
FROM 
(
SELECT Date, treasury_weth_supply as treasury_weth_supply FROM treasury_weth UNION ALL
SELECT Date, 0 as treasury_weth_supply FROM time
) t
GROUP BY 1
),

weth_price AS (
    SELECT avg(price) weth_price, date_trunc('hour', minute) AS "date"
    FROM prices.usd
    WHERE minute >= '2020-09-28'
    and contract_address ='\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
    group by 2),
treasury_lusd AS
(
    SELECT
    evt_block_time as Date,
    -e.value/1e18 as treasury_lusd_supply
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x5f98805A4E8be255a32880FDeC7F6728C6568bA0' -- lusd contract address
    and e."from" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
UNION ALL
    SELECT
    evt_block_time as Date,
    e.value/1e18 as treasury_lusd_supply
    FROM erc20."ERC20_evt_Transfer" e
    WHERE "contract_address" = '\x5f98805A4E8be255a32880FDeC7F6728C6568bA0' -- lusd contract address
    and e."to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
),

final_treasury_lusd as
(
SELECT
date_trunc('day',date) as date,
sum(sum(treasury_lusd_supply)) over (order by date_trunc('day',date)) as treasury_lusd
FROM 
(
SELECT Date, treasury_lusd_supply as treasury_lusd_supply FROM treasury_lusd UNION ALL
SELECT Date, 0 as treasury_lusd_supply FROM time
) t
GROUP BY 1
)

select final_univ2."date", 
(treasury_dai + (slp_treasury/slp_supply)*(lp_dai+(lp_ohm*price)) + treasury_frax + (treasury_univ2/coalesce(NULLIF(univ2_supply,0),1)) * (lp_frax + (f_lp_ohm*price)) + treasury_xsushi * xsushi_price + treasury_weth * weth_price) + treasury_lusd as treasury_mv,
price, treasury_dai, lp_dai, lp_ohm, slp_treasury, slp_supply, treasury_frax, lp_frax, f_lp_ohm, treasury_univ2, univ2_supply, treasury_xsushi, xsushi_price, treasury_weth, weth_price, treasury_lusd
from final_lp_dai
left join final_prices on final_prices."day" = final_lp_dai."date"
left join final_lp_ohm on final_lp_ohm."date" = final_lp_dai."date"
left join final_treasury_dai on final_treasury_dai."date" = final_lp_dai."date"
left join final_slp on final_slp."date" = final_lp_dai."date"
left join final_treasury_slp on final_treasury_slp."date" = final_lp_dai."date"
left join final_lp_frax on final_lp_frax."date" = final_lp_dai."date"
left join final_f_lp_ohm on final_f_lp_ohm."date" = final_lp_dai."date"
left join final_univ2 on final_univ2."date" = final_lp_dai."date"
left join final_treasury_univ2 on final_treasury_univ2."date" = final_lp_dai."date"
left join final_treasury_frax on final_treasury_frax."date" = final_lp_dai."date"
left join final_treasury_xsushi on final_treasury_xsushi."date" = final_lp_dai."date"
left join xsushi_price on xsushi_price."day" = final_lp_dai."date"
left join final_treasury_weth on final_treasury_weth."date" = final_lp_dai."date"
left join weth_price on weth_price."date" = final_lp_dai."date"
left join final_treasury_lusd on final_treasury_lusd."date" = final_lp_dai."date"
where price is Not Null
order by 1 desc 
 


; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.treasury_mv ("date",treasury_mv,price,treasury_dai,lp_dai,lp_ohm,slp_treasury,slp_supply,treasury_frax, lp_frax,f_lp_ohm,treasury_univ2,univ2_supply,treasury_xsushi,xsushi_price,treasury_weth,weth_price,treasury_lusd);
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 59 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.treasury_mv$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;