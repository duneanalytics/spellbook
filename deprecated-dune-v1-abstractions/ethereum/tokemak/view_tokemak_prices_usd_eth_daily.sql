CREATE MATERIALIZED VIEW tokemak.view_tokemak_prices_usd_eth_daily
(
    "date", contract_address,pricing_contract,symbol, price_usd, price_eth
)
AS
(
WITH contracts as(
--select our tokens and then select the tokens which match to a pricing contract so they are in one table
    SELECT DISTINCT address, pricing_contract, symbol FROM (
        SELECT DISTINCT address, pricing_contract, symbol FROM tokemak."view_tokemak_lookup_tokens" WHERE is_pool = false and pricing_contract <> ''
        UNION 
        SELECT DISTINCT address, address as pricing_contract, symbol FROM tokemak."view_tokemak_lookup_tokens" WHERE is_pool = false and pricing_contract = ''
        )as t ORDER BY symbol, address, pricing_contract
),
calendar AS  
        (SELECT i::date as "date"
            ,c.address
            ,c.pricing_contract
            ,c.symbol
        FROM contracts c
        CROSS JOIN generate_series('2021-08-01'::date at time zone 'UTC', current_date, '1 day') t(i) 
 ) , 

main_prices as (
    SELECT DISTINCT ON(date_trunc('day', "minute"), p.contract_address)
    date_trunc('day', "minute") as "date"
    , p.contract_address as pricing_contract
    , p.price
    from prices."usd" p 
    INNER JOIN contracts tl ON tl.pricing_contract = p.contract_address
    WHERE minute > '8/1/2021' and price > .0001 and p.contract_address != '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
    ORDER BY "date" desc,p.contract_address, "minute" desc NULLS LAST),

dex_prices as (
    SELECT DISTINCT ON(date_trunc('day', "hour"), p.contract_address)
    date_trunc('day', "hour") as "date"
    , p.contract_address as pricing_contract
    , p.median_price as price
    from prices."prices_from_dex_data" p INNER JOIN contracts tl ON tl.pricing_contract = p.contract_address
    where "hour" > '8/1/2021' and median_price > .0001 and p.contract_address != '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
    ORDER BY "date" desc,p.contract_address, "hour" desc NULLS LAST
),
steth_prices as (
    select 
        DISTINCT ON(date_trunc('day', "evt_block_time"))
        '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea as pricing_contract,
        date_trunc('day', "evt_block_time") as "date", 
        date_trunc('minute', "evt_block_time") as "minute", 
        CASE WHEN date_trunc('day', "evt_block_time")::date = '2022-05-11'::date THEN .978 ELSE price END as price -- this is done to reflect our exit price so our pca is not overstated
    from (
        select 
            evt_block_time,
            "tokens_sold"/"tokens_bought" as price
        from curvefi."steth_swap_evt_TokenExchange"
        where sold_id = 0  
        union 
        select
            evt_block_time,
            "tokens_bought"/"tokens_sold" as price
        from curvefi."steth_swap_evt_TokenExchange"
        where sold_id = 1 and "tokens_bought" > 0 order by evt_block_time desc
        ) as p
     ORDER BY "date" desc, "minute"  desc NULLS LAST
),
temp as (
    SELECT "date", t.pricing_contract, MAX(price) as price_usd FROM (
        SELECT "date", pricing_contract, price  FROM dex_prices
        UNION
        SELECT "date", pricing_contract, price  FROM main_prices
        UNION
        SELECT "date", pricing_contract, price  FROM steth_prices
    ) as t
     GROUP BY "date", pricing_contract
)
,
eth_prices as (
    SELECT "date", pricing_contract, price_usd as price FROM temp where pricing_contract = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
),
temp1 as (SELECT c."date"
, c.address
, c.pricing_contract
, c.symbol
, CASE WHEN c.pricing_contract = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea THEN t.price_usd * e.price ELSE t.price_usd END as price_usd
, CASE WHEN c.pricing_contract = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'::bytea THEN t.price_usd ELSE t.price_usd/e.price END as price_eth
FROM calendar c
LEFT JOIN eth_prices e on e."date" = c."date"
LEFT JOIN temp t on t."date" = c."date" AND t.pricing_contract = c.pricing_contract
order by "date" desc, symbol asc

),
temp2 as (
    SELECT 
    "date"
    , address
    , pricing_contract
    , symbol
    , price_usd
    , price_eth
    , count(price_usd) OVER (PARTITION BY address ORDER BY "date") AS grpUSD
    , count(price_eth) OVER (PARTITION BY address ORDER BY "date") AS grpETH
    FROM temp1
    order by "date" desc, symbol asc
)

    SELECT
    "date"
    , address
    , pricing_contract
    , symbol
    , first_value(price_usd) OVER (PARTITION BY address, grpUSD ORDER BY "date") AS price_usd
    , first_value(price_eth) OVER (PARTITION BY address, grpETH ORDER BY "date") AS price_eth
    FROM temp2 ORDER BY "date" desc, symbol

);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_prices_usd_eth_daily (
   "date",
   contract_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('15 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_prices_usd_eth_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
