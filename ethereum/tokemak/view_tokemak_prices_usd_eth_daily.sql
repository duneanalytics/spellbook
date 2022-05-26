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
        CROSS JOIN generate_series('2021-08-01'::date, current_date, '1 day') t(i) 
 ) , 

main_prices as (
    SELECT DISTINCT ON(date_trunc('day', "minute"), p.contract_address)
    date_trunc('day', "minute") as "date", p.contract_address as pricing_contract, p.price
    from prices."usd" p 
    INNER JOIN contracts tl ON tl.pricing_contract = p.contract_address
    WHERE minute > '8/1/2021' and price > .0001
    ORDER BY "date" desc,p.contract_address, "minute" desc NULLS LAST),

dex_prices as (
    SELECT DISTINCT ON(date_trunc('day', "hour"), p.contract_address)
    date_trunc('day', "hour") as "date", p.contract_address as pricing_contract, p.median_price as price
    from prices."prices_from_dex_data" p INNER JOIN contracts tl ON tl.pricing_contract = p.contract_address
    where "hour" > '8/1/2021' and median_price > .0001
    ORDER BY "date" desc,p.contract_address, "hour" desc NULLS LAST
),
temp as (
    SELECT "date"::date, t.pricing_contract, MAX(price) as price_usd FROM (
        SELECT "date", pricing_contract, price  FROM dex_prices
        UNION
        SELECT "date", pricing_contract, price  FROM main_prices
    ) as t
     GROUP BY "date", pricing_contract
),
eth_prices as (
    SELECT "date", pricing_contract, price_usd as price FROM temp where pricing_contract = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
)
SELECT c."date"::date,  c.address, c.pricing_contract,c.symbol, t.price_usd, t.price_usd/e.price as price_eth FROM calendar c
INNER JOIN eth_prices e on e."date" = c."date"
INNER JOIN temp t on t."date" = c."date" AND t.pricing_contract = c.pricing_contract
order by "date" desc, symbol asc
);

CREATE UNIQUE INDEX ON tokemak.view_tokemak_prices_usd_eth_daily (
   "date",
   contract_address
);

INSERT INTO cron.job(schedule, command)
VALUES ('* 1 * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_prices_usd_eth_daily$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
