CREATE OR REPLACE FUNCTION chainlink.insert_price_feeds(start_block_time timestamptz, end_block_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
-- get a series of the proxy "basic
WITH gs AS (
SELECT hr, oa.feed_name, oa.address, oa.proxy, "underlying_token_address"
FROM (
    SELECT
    generate_series (
    DATE_TRUNC('day', start_block_time ),
    DATE_TRUNC('hour', end_block_time ),
    '1 hour'
    ) AS hr
    , "feed_name"
    ,"proxy"
    ,address
    FROM chainlink.oracle_addresses
    ) oa
LEFT JOIN chainlink.oracle_token_mapping c
ON c.proxy = oa.proxy
)

, feed_updates AS (
SELECT
dt, c.feed_name, c.price, c.proxy, c.address, underlying_token_address
, c.price::decimal/(10^extra_decimals)::decimal AS underlying_token_price
FROM (
	SELECT
	DATE_TRUNC('hour',block_time) AS dt
	, feed_name
	, AVG(bytea2numeric(topic2)::decimal/(10^decimals)::decimal) AS price
	,"proxy", "address"
	FROM optimism.logs l
	INNER JOIN chainlink.oracle_addresses cfa
	    ON l.contract_address = cfa.address
	WHERE topic1 = '\x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f' --Answer Updated
	AND contract_address IN (SELECT address FROM chainlink.oracle_addresses)
	AND block_time >= start_block_time
	AND block_time < end_block_time
	GROUP BY 1,2, 4,5
	) c
LEFT JOIN chainlink.oracle_token_mapping o
	ON c.proxy = o.proxy
),

rows AS (
    INSERT INTO chainlink.view_price_feeds (
        hour,
    	feed_name,
        address,
        proxy,
    	price,
    	underlying_token_address,
    	underlying_token_price
    )

SELECT --avg in case there are multiple overlapping feeds
	hour, feed_name, address, proxy, AVG(price) AS price, underlying_token_address, AVG(underlying_token_price) AS underlying_token_price
FROM (
SELECT hr AS hour, address, proxy, feed_name, underlying_token_address
, first_value(price) OVER (PARTITION BY feed_name, grp ORDER BY hr) AS price
, first_value(underlying_token_price) OVER (PARTITION BY feed_name, grp ORDER BY hr) AS underlying_token_price

FROM (
SELECT hr, feed_name, address, proxy, price, underlying_token_address, underlying_token_price,
count(price) OVER (PARTITION BY feed_name ORDER BY hr) AS grp
FROM (
    SELECT gs.hr, gs.feed_name, gs.address, gs.proxy, price, gs.underlying_token_address, underlying_token_price
    FROM gs
    LEFT JOIN feed_updates f
        ON gs.hr = f.dt
	AND gs.underlying_token_address = f.underlying_token_address

-- Union with the most recent prices to pull forward prices 
    UNION ALL
	SELECT hour, feed_name, address, proxy, price, underlying_token_address, underlying_token_price
    	FROM (
		SELECT hour, feed_name, address, proxy, price, underlying_token_address, underlying_token_price,
    			DENSE_RANK() OVER (PARTITION BY feed_name, address, proxy, underlying_token_address ORDER BY hour DESC) AS h_rank
    			FROM chainlink.view_price_feeds v
    		WHERE hour >= start_block_time - interval '1 day'
		AND hour < end_block_time
		AND NOT EXISTS (SELECT 1 FROM feed_updates f WHERE f.dt = v.hour AND f.feed_name = v.feed_name) -- doesn't have an update
        	) old
    	WHERE h_rank = 1 -- most recent update
    	) str
    ) uni
) a
	
WHERE price IS NOT NULL
AND hour >= DATE_TRUNC('hour',start_block_time)
AND hour <= DATE_TRUNC('hour',end_block_time)
	
GROUP BY hour, feed_name, underlying_token_address, address, proxy

ON CONFLICT (hour,feed_name,underlying_token_address)
    DO UPDATE SET
        price = EXCLUDED.price,
	underlying_token_price = EXCLUDED.underlying_token_price
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- --delete prior to backfill reload (uncomment as needed)
-- DELETE FROM chainlink.view_price_feeds;

-- -- fill to start
-- SELECT chainlink.insert_price_feeds(
--     '2021-11-11'::date,
--     '2022-03-16'::date
-- )
-- ;

SELECT chainlink.insert_price_feeds(
    '2022-03-16'::date,
    now()
)
;

/*
WHERE NOT EXISTS (
    SELECT *
    FROM chainlink.view_price_feeds
    LIMIT 1
);
INSERT INTO cron.job (schedule, command)
VALUES ('15,30,45,59 * * * *', $$
    SELECT chainlink.insert_price_feeds(
        (SELECT MAX(hour) - interval '3 days' FROM chainlink.view_price_feeds), --buffer in case the db gets stuck
        now() + interval '1 hour' -- to pull prices in to the next hour if needed
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
