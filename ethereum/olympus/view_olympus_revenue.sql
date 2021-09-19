CREATE SCHEMA IF NOT EXISTS olympus;

BEGIN;
DROP materialized VIEW IF EXISTS olympus.olympus_revenue;
create materialized view olympus.olympus_revenue as 

 with time as
(
SELECT
generate_series('2021-03-31 00:00', NOW(), '1 day'::interval) as Date
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

   price as ( SELECT
        a1_prcs."day" AS "date", 
        (AVG((a1_amt/a0_amt)*a1_prc)) AS price
    FROM swap 
    JOIN a1_prcs ON swap."day" = a1_prcs."day"
    GROUP BY 1
    ORDER BY 1 desc),
    
slp_deposits_data as (select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c'
and "to" = '\x13E8484a86327f5882d1340ed0D7643a29548536'
group by 1

UNION ALL

select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c'
and "to" = '\xd27001d1aAEd5f002C722Ad729de88a91239fF29'
group by 1

UNION ALL

select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c'
and "to" = '\x996668C46Fc0B764aFdA88d83eB58afc933a1626'
group by 1

UNION ALL

select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c'
and "to" = '\x956c43998316b6a2F21f89a1539f73fB5B78c151'
group by 1
),

slp_deposits as (
select day, sum(amount) as slp_deposited
from slp_deposits_data
group by 1),

slp_deposits_with_gap as (select "date", coalesce(slp_deposited,0) as slp_deposited
from time
left join slp_deposits on "date" = day),

slp AS
(
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c' -- slp contract address
    and e."to" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
),

final_slp as
(
SELECT
Date as Date,
sum(sum(supply)) over (order by Date) as slp_supply
FROM 
(
SELECT Date, slp.supply as supply FROM slp UNION ALL
SELECT Date, 0 as supply FROM time
) t
GROUP BY 1
),

lp_dai AS
(
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(-sum(e.value/1e18), 0) as lp_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."from" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as lp_dai_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x6b175474e89094c44da98b954eedeac495271d0f' -- OHM contract address
    and e."to" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
),

final_lp_dai as
(
SELECT
Date as Date,
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
    date_trunc('day', block_time) as Date,
    COALESCE(-sum(e.value/1e9), 0) as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."from" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(e.value/1e9), 0) as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."to" = '\x34d7d7Aaf50AD4944B70B320aCB24C95fa2def7c'
    GROUP BY 1
),

final_lp_ohm as
(
SELECT
Date as Date,
sum(sum(lp_ohm_supply)) over (order by Date) as lp_ohm
FROM 
(
SELECT Date, lp_ohm_supply as lp_ohm_supply FROM lp_ohm UNION ALL
SELECT Date, 0 as lp_ohm_supply FROM time
) t
GROUP BY 1
),

slp_bonded as (select slp_deposits_with_gap."date", slp_deposited, (lp_ohm*price + lp_dai)*(slp_deposited/slp_supply) as mv_slp_bonded_ohmdai
from slp_deposits_with_gap
left join final_lp_ohm on final_lp_ohm."date" = slp_deposits_with_gap."date"
left join final_lp_dai on final_lp_dai."date" = slp_deposits_with_gap."date"
left join final_slp on final_slp."date" = slp_deposits_with_gap."date"
left join price on price."date" = slp_deposits_with_gap."date"

),

univ2_deposits_data as (select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
and "to" = '\x539b6c906244Ac34E348BbE77885cdfa994a3776'
group by 1
UNION ALL
select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
and "to" = '\xc20CffF07076858a7e642E396180EC390E5A02f7'
group by 1
),

univ2_deposits as (
select day, sum(amount) as univ2_deposited
from univ2_deposits_data
group by 1),

univ2_deposits_with_gap as (select "date", coalesce(univ2_deposited,0) as univ2_deposited
from time
left join univ2_deposits on "date" = day),

univ2 AS
(
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877' -- univ2 contract address
    and e."from" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(-e.value/1e18), 0) as supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877' -- univ2 contract address
    and e."to" = '\x0000000000000000000000000000000000000000'
    GROUP BY 1
),

final_univ2 as
(
SELECT
Date as Date,
sum(sum(supply)) over (order by Date) as univ2_supply
FROM 
(
SELECT Date, univ2.supply as supply FROM univ2 UNION ALL
SELECT Date, 0 as supply FROM time
) t
GROUP BY 1
),

lp_frax AS
(
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(-sum(e.value/1e18), 0) as lp_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."from" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(e.value/1e18), 0) as lp_frax_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x853d955acef822db058eb8505911ed77f175b99e' -- FRAX contract address
    and e."to" = '\x2dcE0dDa1C2f98e0F171DE8333c3c6Fe1BbF4877'
    GROUP BY 1
),

final_lp_frax as
(
SELECT
Date as Date,
sum(sum(lp_frax_supply)) over (order by Date) as lp_frax
FROM 
(
SELECT Date, lp_frax_supply as lp_frax_supply FROM lp_frax UNION ALL
SELECT Date, 0 as lp_frax_supply FROM time
) t
GROUP BY 1
),

lp_f_ohm AS
(
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(-sum(e.value/1e9), 0) as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."from" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    GROUP BY 1
UNION ALL
    SELECT
    date_trunc('day', block_time) as Date,
    COALESCE(sum(e.value/1e9), 0) as lp_ohm_supply
    FROM erc20."ERC20_evt_Transfer" e
    LEFT JOIN ethereum."transactions" tx ON evt_tx_hash = tx.hash
    WHERE "contract_address" = '\x383518188c0c6d7730d91b2c03a03c837814a899' -- OHM contract address
    and e."to" = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'
    GROUP BY 1
),

final_f_lp_ohm as
(
SELECT
Date as Date,
sum(sum(lp_ohm_supply)) over (order by Date) as lp_ohm
FROM 
(
SELECT Date, lp_ohm_supply as lp_ohm_supply FROM lp_f_ohm UNION ALL
SELECT Date, 0 as lp_ohm_supply FROM time
) t
GROUP BY 1
),

univ2_bonded as (select univ2_deposits_with_gap."date", univ2_deposited, (lp_ohm * price + lp_frax)*(univ2_deposited/coalesce(nullif(univ2_supply,0),1)) as mv_univ2_bonded_ohmfrax
from univ2_deposits_with_gap
left join final_f_lp_ohm on final_f_lp_ohm."date" = univ2_deposits_with_gap."date"
left join final_lp_frax on final_lp_frax."date" = univ2_deposits_with_gap."date"
left join final_univ2 on final_univ2."date" = univ2_deposits_with_gap."date"
left join price on price."date" = univ2_deposits_with_gap."date"
),

lp_bonds_mv as (select slp_bonded."date", mv_slp_bonded_ohmdai, mv_univ2_bonded_ohmfrax
from slp_bonded
left join univ2_bonded on univ2_bonded."date" = slp_bonded."date"
order by 1 desc),

dai_deposits_data as (select date_trunc('day', call_block_time) as day, sum("amount_"/1e18) as amount
from olympus."OlympusDAIDepository_call_deposit"
group by 1
UNION ALL
select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x6b175474e89094c44da98b954eedeac495271d0f'
and "to" = '\x13E8484a86327f5882d1340ed0D7643a29548536'
group by 1
UNION ALL
select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x6b175474e89094c44da98b954eedeac495271d0f'
and "to" = '\xD03056323b7a63e2095AE97fA1AD92E4820ff045'
group by 1
UNION ALL
select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x6b175474e89094c44da98b954eedeac495271d0f'
and "to" = '\x575409F8d77c12B05feD8B455815f0e54797381c'
group by 1
),

dai_deposits as(select day, sum(amount) as dai
from dai_deposits_data
group by day),

dai_bonded as (select "date", coalesce(dai,0) as dai_bonded
from time
left join dai_deposits on "date" = "day"),

frax_deposits_data as (select date_trunc('day', evt_block_time) as day, sum(value/1e18) as amount
from erc20."ERC20_evt_Transfer"
where contract_address = '\x853d955acef822db058eb8505911ed77f175b99e'
and "to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
group by 1
),

frax_deposits as(select day, sum(amount) as frax
from frax_deposits_data
group by day),

frax_bonded as (select "date", coalesce(frax,0) as frax_bonded
from time
left join frax_deposits on "date" = "day"),

reserve_bonds as (select frax_bonded."date", dai_bonded,frax_bonded
from dai_bonded
left join frax_bonded on frax_bonded."date" = dai_bonded."date"
order by 1 desc),

xsushi as (select date_trunc('day', "evt_block_time") as day, sum(value/1e18) as xsushi
from erc20."ERC20_evt_Transfer"
where "to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
and contract_address = '\x8798249c2e607446efb7ad49ec89dd1865ff4272'
group by 1),

weth as (select date_trunc('day', "evt_block_time") as day, sum(value/1e18) as weth
from erc20."ERC20_evt_Transfer"
where "to" = '\x31F8Cc382c9898b273eff4e0b7626a6987C846E8'
and contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
group by 1),

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
    
data_sushi as (select distinct on (tx_hash) block_time,  usd_amount
from dex.trades
where exchange_contract_address = '\x34d7d7aaf50ad4944b70b320acb24c95fa2def7c'),

data_uni as (select distinct on (tx_hash) block_time,  case when token_a_address = '\x853d955acef822db058eb8505911ed77f175b99e' then token_a_amount
when token_b_address = '\x853d955acef822db058eb8505911ed77f175b99e' then token_b_amount end usd_amount
from dex.trades
where exchange_contract_address = '\x2dce0dda1c2f98e0f171de8333c3c6fe1bbf4877'),

sushi_fees as (select date_trunc('day', block_time) as day, sum(usd_amount * 0.0025) as ohmdai_fees
from data_sushi
group by 1),

uni_fees as (select date_trunc('day', block_time) as day, sum(usd_amount * 0.003) as ohmfrax_fees
from data_uni
group by 1),

final_data as (select s.day, (slp_treasury/slp_supply) * ohmdai_fees as ohmdai_fees, 
(treasury_univ2/coalesce(NULLIF(univ2_supply,0),1)) * ohmfrax_fees as ohmfrax_fees,
(slp_treasury/slp_supply) * ohmdai_fees + coalesce((treasury_univ2/coalesce(NULLIF(univ2_supply,0),1)) * ohmfrax_fees, 0) as total_fees,
weth_price
from sushi_fees s
left join uni_fees u on u.day = s.day
left join dune_user_generated.treasury_mv tm on tm."date" = s.day),

final_fees as (select day, ohmdai_fees, ohmfrax_fees, total_fees,
sum(total_fees) over (order by day asc rows between unbounded preceding and current row) as running_total,
weth_price
from final_data
order by 1 desc),

final as (select lp."date", mv_slp_bonded_ohmdai, mv_univ2_bonded_ohmfrax, dai_bonded, frax_bonded,
(mv_slp_bonded_ohmdai + mv_univ2_bonded_ohmfrax + dai_bonded + frax_bonded) as bond_revenue,
coalesce(xsushi * xsushi_price, 0) as xsushi_revenue,
coalesce(weth * weth_price,0) as weth_revenue,
total_fees,
(mv_slp_bonded_ohmdai + mv_univ2_bonded_ohmfrax + dai_bonded + frax_bonded + coalesce(xsushi * xsushi_price, 0) + coalesce(weth * weth_price, 0) + total_fees) as total_revenue
from lp_bonds_mv lp
left join reserve_bonds rb on rb."date" = lp."date" 
left join xsushi xt on xt.day = lp."date"
left join xsushi_price xp on xp.day = lp."date"
left join final_fees ff on ff.day = lp."date"
left join weth on weth.day = lp."date"
)

select *, 
AVG(total_revenue) OVER(ORDER BY "date" ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS "total_revenue_7d_moving_avg"
from final

; 

CREATE INDEX IF NOT EXISTS "date" ON olympus.olympus_revenue ("date", mv_slp_bonded_ohmdai, apy,mv_univ2_bonded_ohmfrax,dai_bonded,frax_bonded,bond_revenue, xsushi_revenue,weth_revenue,total_fees,total_revenue,"total_revenue_7d_moving_avg");
COMMIT;

INSERT INTO cron.job(schedule, command)
VALUES ('* 1 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY olympus.olympus_revenue$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
