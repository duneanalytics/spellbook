CREATE OR REPLACE FUNCTION chainlink.insert_price_feeds(start_block_time timestamptz, end_block_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO chainlink.view_price_feeds (
        hour,
    	feed_name,
    	price,
    	underlying_token_address
    )
    
WITH gs AS (
    SELECT
    generate_series (
    DATE_TRUNC('day', start_block_time ),
    DATE_TRUNC('hour', end_block_time ),
    '1 hour'
    ) AS hr
    , "feed_name"
    FROM chainlink.oracle_addresses
)

, feed_updates AS (
WITH feeds AS (
	SELECT
	DATE_TRUNC('hour',block_time) AS dt
	, feed_name
	, AVG(bytea2numeric(topic2)::decimal/(10^decimals)::decimal) AS price
	,"proxy", "address", underlying_token_address
	FROM optimism.logs l
	INNER JOIN chainlink.oracle_addresses cfa
	    ON l.contract_address = cfa.address
	WHERE topic1 = '\x0559884fd3a460db3073b7fc896cc77986f16e378210ded43186175bf646fc5f' --Answer Updated
	AND contract_address IN (SELECT address FROM chainlink.oracle_addresses)
	GROUP BY 1,2, 4,5,6
	)
SELECT * FROM feeds
	UNION ALL
-- add WETH as a copy of ETH
SELECT  dt, feed_name, price ,"proxy", "address", '\x4200000000000000000000000000000000000006' AS underlying_token_address
	FROM feeds WHERE underlying_token_address = '\xDeadDeAddeAddEAddeadDEaDDEAdDeaDDeAD0000'
)

SELECT --avg in case there are multiple overlapping feeds
	hour, feed_name, AVG(price) AS price, underlying_token_address
FROM (
SELECT hr AS hour, feed_name
, first_value(price) OVER (PARTITION BY feed_name, grp ORDER BY hr) AS price

, first_value(underlying_token_address) OVER (PARTITION BY feed_name, grp ORDER BY hr) AS underlying_token_address
FROM (
SELECT hr, feed_name, price, underlying_token_address,
count(price) OVER (PARTITION BY feed_name ORDER BY hr) AS grp
FROM (
    SELECT gs.hr, gs.feed_name, price, underlying_token_address
    FROM gs
    LEFT JOIN feed_updates f
        ON gs.hr = f.dt
        AND gs.feed_name = f.feed_name

-- Union with the most recent prices to pull forward prices 
    UNION ALL
	SELECT hour, feed_name, price, underlying_token_address
    	FROM (
    		SELECT hour, feed_name, price, underlying_token_address,
    			DENSE_RANK() OVER (PARTITION BY feed_name, underlying_token_address ORDER BY hour DESC) AS h_rank
    			FROM chainlink.view_price_feeds
    		WHERE hour >= start_block_time - interval '1 day'
        	) old
    	WHERE h_rank = 1
    	) str
    ) uni
) a
WHERE price IS NOT NULL
GROUP BY 1,2, 4

ON CONFLICT (hour,feed_name,underlying_token_address)
    DO UPDATE SET
        price = EXCLUDED.price
	
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

DELETE FROM chainlink.view_price_feeds;
-- fill to start
SELECT chainlink.insert_price_feeds(
    '2021-11-11'::date,
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM chainlink.view_price_feeds
    LIMIT 1
);
/*
INSERT INTO cron.job (schedule, command)
VALUES ('15,30,45,59 * * * *', $$
    SELECT chainlink.insert_price_feeds(
        (SELECT MAX(hour) - interval '3 days' FROM chainlink.view_price_feeds), --buffer in case the db gets stuck
        now() + interval '1 hour' -- to pull prices in to the next hour if needed
        );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
