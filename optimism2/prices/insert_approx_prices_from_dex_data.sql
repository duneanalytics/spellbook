CREATE OR REPLACE FUNCTION prices.insert_approx_prices_from_dex_data(start_time timestamptz, end_time timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH 

hour_gs AS (
SELECT generate_series(DATE_TRUNC('hour',start_time) , DATE_TRUNC('hour',end_time) , '1 hour') AS hour
)

, starting_prices AS ( --carry over previous prices if no trades (to avoid gaps)
	WITH last_updates AS (
		SELECT        
		contract_address,
		MAX(hour) AS last_hour
		FROM prices.approx_prices_from_dex_data
		WHERE median_price IS NOT NULL
		AND hour <= (start_time - interval '3 days')
		AND contract_address != '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' --Raw ETH, gets auto-filled in from WETH later
		GROUP BY 1
		)
	SELECT p.contract_address AS token, DATE_TRUNC('hour',(start_time - interval '3 days')) AS hour, p.median_price, 1 AS num_samples, p.symbol, p.decimals
	FROM prices.approx_prices_from_dex_data p
	INNER JOIN last_updates u
		ON u.contract_address = p.contract_address
		AND u.last_hour = p.hour
	GROUP BY 1,2,3,4,5,6
)

, dex_price_stables AS (
--for tokens where dune doesn't have the price, calculate the median price, assuming USDT, DAI, USDC = 1
SELECT
hour,
token, symbol, decimals,
CASE WHEN symbol IN ('USDT','DAI','USDC') THEN 1
ELSE median_price
END AS median_price
,num_samples
FROM
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY token ORDER BY hour DESC) AS hrank

FROM
(
SELECT
DATE_TRUNC('hour',block_time) AS hour,
token, symbol, decimals,
percentile_cont(0.5) WITHIN GROUP (ORDER BY token_price) AS median_price,
COUNT(*) AS num_samples
FROM (
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT --tokena
        t.block_time, t.exchange_contract_address,
        ea.symbol, ea.decimals,
        t.token_a_address AS token, --t.token_b_address,
        t.token_a_amount_raw/(10^ea.decimals) AS token_amount,
        --t.token_b_amount_raw/(10^eb.decimals) AS token_b_amount,
        CASE WHEN eb.symbol IN ('USDT','DAI','USDC') THEN --assume price = 1
            t.token_b_amount_raw/(10^eb.decimals) ELSE NULL
            END AS usd_amount

        FROM dex.trades t
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        WHERE project IN ('Uniswap','1inch')
	AND t.block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
		AND t.block_time <= end_time
        
        AND t.token_a_amount_raw > 100 --min to exclude weird stuff
    ) tokena
    
    UNION ALL
    
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT --tokenb
        t.block_time, t.exchange_contract_address,
        eb.symbol, eb.decimals,
        t.token_b_address AS token,

        t.token_b_amount_raw/(10^eb.decimals) AS token_amount,
        CASE WHEN ea.symbol IN ('USDT','DAI','USDC') THEN --assume price = 1
        t.token_a_amount_raw/(10^ea.decimals) ELSE NULL
        END AS usd_amount
        
        FROM dex.trades t
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        WHERE project IN ('Uniswap','1inch')
	AND t.block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
		AND t.block_time <= end_time
        
        AND t.token_b_amount_raw > 100 --min to exclude weird stuff
    ) tokenb
    
) a
WHERE token_price > 0
GROUP BY 1,2,3, 4

UNION ALL

SELECT
    '01-01-2000' AS hour, token::bytea, symbol, decimals, 1 AS median_price,1 AS num_samples
    FROM ( values
            ('\x7f5c764cbc14f9669b88837ca1490cca17c31607','USDC',6)
            ,('\x94b008aa00579c1307b0ef2c499ad98a8ce58e58','USDT',6)
            ,('\xda10009cbd5d07dd0cecc66161fc93d7c9000da1','DAI',18)
        ) t (token, symbol, decimals)

) b

) c
--WHERE hrank = 1 -- holdover if we want to turn this to latest price
)

, hour_token_dex_stables_gs AS (
WITH token_list AS (
    SELECT token, symbol, decimals FROM dex_price_stables 
    GROUP BY 1,2,3
    )

SELECT 
hour, token, symbol, decimals
FROM
token_list, hour_gs

)

, prices_vs_stables AS (
SELECT
hour, token, symbol, decimals, CASE WHEN median_price IS NOT NULL THEN 1 ELSE NULL END AS "window" --if this was this when trades actually happened
    , first_value(median_price) OVER (PARTITION BY token, grp ORDER BY hour) AS median_price
    , first_value(num_samples) OVER (PARTITION BY token, grp ORDER BY hour) AS num_samples
     
FROM (
    SELECT 
    gs.hour, gs.token, gs.symbol, gs.decimals, p.median_price, p.num_samples, 
        count(p.median_price) OVER (PARTITION BY gs.token ORDER BY gs.hour) AS grp
    FROM hour_token_dex_stables_gs gs
    LEFT JOIN dex_price_stables p
        ON gs.hour = p.hour
        AND gs.token = p.token
        AND gs.symbol = p.symbol
        AND gs.decimals = p.decimals
    ) fill

)

--Use ETH Price to calculate other tokens that are only traded vs ETH, or more often traded vs eth

, dex_price_weth AS(

SELECT
hour,
token, symbol, decimals,
CASE WHEN symbol IN ('USDT','DAI','USDC') THEN 1
ELSE median_price
END AS median_price
,num_samples
FROM
(
SELECT *,
DENSE_RANK() OVER (PARTITION BY token ORDER BY hour DESC) AS hrank

FROM
(
SELECT
DATE_TRUNC('hour',block_time) AS hour,
token, symbol, decimals,
percentile_cont(0.5) WITHIN GROUP (ORDER BY token_price) AS median_price,
COUNT(*) AS num_samples
FROM (
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT --tokena
        t.block_time, t.exchange_contract_address,
        ea.symbol, ea.decimals,
        t.token_a_address AS token, 
        t.token_a_amount_raw/(10^ea.decimals) AS token_amount,
        
            t.token_b_amount_raw/(10^eb.decimals) * dp.median_price --#eth * latestusd
             AS usd_amount

        FROM dex.trades t
        INNER JOIN uniswap_v3.view_pools p ON
        t."exchange_contract_address" = p.pool
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        INNER JOIN prices_vs_stables dp ON --latest eth price
            t.token_b_address = dp.token
            AND dp.hour = DATE_TRUNC('hour',t.block_time)

        WHERE t.token_a_amount_raw > 100 --min to exclude weird stuff
        AND t.token_b_address = '\x4200000000000000000000000000000000000006' -- weth
	AND t.block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
		AND t.block_time <= end_time

    ) tokena
    
    UNION ALL
    
    SELECT *,
    usd_amount/token_amount AS token_price
    FROM
    (
        SELECT 
        t.block_time, t.exchange_contract_address,
        eb.symbol, eb.decimals,
        t.token_b_address AS token,
        t.token_b_amount_raw/(10^eb.decimals) AS token_amount,
        t.token_a_amount_raw/(10^ea.decimals) * dp.median_price --#eth * latestusd
         AS usd_amount
        FROM dex.trades t
        INNER JOIN uniswap_v3.view_pools p ON
        t."exchange_contract_address" = p.pool
        INNER JOIN erc20."tokens" ea --both need to have known decimals, we're not going to assume anything.
        ON ea."contract_address" = t.token_a_address
        INNER JOIN erc20."tokens" eb
        ON eb."contract_address" = t.token_b_address
        INNER JOIN prices_vs_stables dp ON --latest eth price
            t.token_a_address = dp.token
            AND dp.hour = DATE_TRUNC('hour',t.block_time)
        WHERE t.token_b_amount_raw > 100 --min to exclude weird stuff
        AND t.token_a_address = '\x4200000000000000000000000000000000000006' --weth
	AND t.block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
		AND t.block_time <= end_time
    ) tokenb
    
) a
WHERE token_price > 0
GROUP BY 1,2,3, 4

) b

) c
--WHERE hrank = 1 -- holdover if we want to turn this to latest price
WHERE num_samples > 1 -- exclude low sample updates for DEX trades
)

, dex_price_synths AS (
--Seems like there isn't a set swap event, so we take tokens to and tokens from the sender within "trade" transactions to calculate the exchange rate.
WITH token_sent AS (
    SELECT evt_block_time, evt_tx_hash, r.contract_address AS token, r.value, decimals, symbol FROM optimism.transactions t
        INNER JOIN erc20."ERC20_evt_Transfer" r
            ON t.hash = r.evt_tx_hash
            AND t."from" = r."from"
        INNER JOIN erc20."tokens" e
            ON e."contract_address" = r."contract_address"
        WHERE t."to" = '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'
        AND substring(data from 1 for 4) = '\x30ead760' --methodid
	AND r.evt_block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
		AND r.evt_block_time <= end_time

    )
, token_received AS (
    SELECT evt_block_time, evt_tx_hash, r.contract_address AS token, r.value, decimals, symbol FROM optimism.transactions t
        INNER JOIN erc20."ERC20_evt_Transfer" r
            ON t.hash = r.evt_tx_hash
            AND t."from" = r."to"
        INNER JOIN erc20."tokens" e
            ON e."contract_address" = r."contract_address"
        WHERE t."to" = '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'
        AND substring(data from 1 for 4) = '\x30ead760' --methodid
	AND r.evt_block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
		AND r.evt_block_time <= end_time
        
    )
--	This negatively impacts runtime for some reason. To be figured out later.
--     , susd_fees AS ( --the user pays the fees, but this shouldn't go in to the price conversion, so we subtract from the sUSD total
--     SELECT evt_block_time, evt_tx_hash, r.contract_address AS token, r.value, decimals, symbol FROM erc20."ERC20_evt_Transfer" r
--         INNER JOIN optimism.transactions t
--             ON t.hash = r.evt_tx_hash
--         INNER JOIN erc20."tokens" e
--             ON e."contract_address" = r."contract_address"
--         WHERE t."to" = '\x8700daec35af8ff88c16bdf0418774cb3d7599b4'
--         AND substring(data from 1 for 4) = '\x30ead760' --methodid
--         AND r."from" = '\x0000000000000000000000000000000000000000'
--         AND r."to" = '\xfeefeefeefeefeefeefeefeefeefeefeefeefeef'
--         AND r."contract_address" = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9'
-- 	AND r.evt_block_time >= DATE_TRUNC('hour',start_time - interval '3 days') --3 day buffer to catch tokens which may not have had a recent trade
-- 	    AND r.evt_block_time <= end_time
        
--     )
, ratios AS (
    SELECT
    s.evt_block_time, s.evt_tx_hash, s.decimals AS s_decimals, r.decimals AS r_decimals,
    r.token AS r_token, r.symbol AS r_symbol, s.token AS s_token, s.symbol AS s_symbol,
    CASE WHEN r.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN
        ((r.value/*-f.value*/)
	 	/(10^r.decimals))::decimal/(s.value/(10^s.decimals))::decimal
    WHEN s.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN
        ((s.value/*-f.value*/)
	 	/(10^s.decimals))::decimal/(r.value/(10^r.decimals))::decimal 
    END AS price_ratio,
    CASE WHEN r.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN 'sent' --sent token, received sUSD
        WHEN s.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9' THEN 'received' -- received token, sent sUSD
        ELSE '0' --ignore
        END AS token_side
    FROM token_sent s
    INNER JOIN token_received r
        ON s.evt_tx_hash = r.evt_tx_hash
--     INNER JOIN susd_fees f
--         ON s.evt_tx_hash = f.evt_tx_hash

    )
    
SELECT *
FROM (
SELECT pps.hour, pps.token, pps.symbol, pps.decimals,
pps.median_price * sp.median_price AS median_price, pps.num_samples,
DENSE_RANK() OVER (PARTITION BY pps.token ORDER BY pps.hour DESC) AS h_rank
FROM (
    SELECT
    DATE_TRUNC('hour',evt_block_time) AS hour,
    CASE WHEN token_side = 'sent' THEN s_token ELSE r_token END AS token,
    CASE WHEN token_side = 'sent' THEN s_symbol ELSE r_symbol END AS symbol,
    CASE WHEN token_side = 'sent' THEN s_decimals ELSE r_decimals END AS decimals,
    
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_ratio) AS median_price, -- already flipped
    COUNT(*) AS num_samples
    
    FROM ratios
    WHERE token_side != '0'
    GROUP BY 1,2,3,4
    
    ) pps
    INNER JOIN prices_vs_stables sp ON --latest eth price
            sp.token = '\x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9'
            AND sp.hour = pps.hour

    ) rnk
--WHERE h_rank = 1
WHERE num_samples > 1 -- exclude low sample updates for DEX trades
)

, dex_price_bridge_tokens AS (
SELECT DATE_TRUNC('hour', pr.hour) AS hour, "bridge_token" AS token, "bridge_symbol" AS symbol, "bridge_decimals" AS decimals, median_price * price_ratio AS median_price, p.num_samples,
DENSE_RANK() OVER (PARTITION BY bridge_token ORDER BY pr.hour DESC) AS hrank

FROM prices.hourly_bridge_token_price_ratios pr

INNER JOIN prices_vs_stables p
        ON pr.erc20_token = p.token
        AND DATE_TRUNC('hour',pr.hour) = p.hour

)


, price AS (
WITH get_best_price_estimate AS (
SELECT hour, token, symbol, decimals, median_price, num_samples, rnk
    FROM (    
        SELECT *, ROW_NUMBER() OVER (PARTITION BY hour, token ORDER BY num_samples DESC, rnk ASC) AS p_rank --pick which price to take
        FROM (
            SELECT hour, token, symbol, decimals, median_price, num_samples, 1 AS rnk FROM prices_vs_stables WHERE "window" = 1 --when trades happened
		UNION ALL
            SELECT hour, token, symbol, decimals, median_price, num_samples, 2 AS rnk FROM dex_price_weth
		UNION ALL
            SELECT hour, token, symbol, decimals, median_price, num_samples, -1 AS rnk FROM dex_price_synths --always use synths for susd
		UNION ALL
            SELECT hour, token, symbol, decimals, median_price, num_samples, -1 AS rnk FROM dex_price_bridge_tokens --bridge tokens
        	UNION ALL
            SELECT hour, token, symbol, decimals, median_price, num_samples, 99 AS rnk FROM starting_prices --starting point if null
            ) a
            WHERE median_price IS NOT NULL
        ) r
    WHERE p_rank = 1
    )
SELECT hour, token, symbol, decimals, median_price, num_samples, rnk FROM get_best_price_estimate
UNION ALL
SELECT hour, '\xdeaddeaddeaddeaddeaddeaddeaddeaddead0000' AS token, 'ETH' AS symbol, 18 AS decimals, median_price, num_samples, rnk
FROM get_best_price_estimate WHERE token = '\x4200000000000000000000000000000000000006'

)

--Fill in gaps

, hour_token_gs AS (
WITH token_list AS (
    SELECT token, symbol, decimals FROM price 
    GROUP BY 1,2,3
    )

SELECT 
hour, token, symbol, decimals
FROM
token_list, hour_gs

)

--logic to fill in gaps https://dba.stackexchange.com/questions/186218/carry-over-long-sequence-of-missing-values-with-postgres
, final_prices AS (
SELECT
token AS contract_address, hour
, first_value(median_price) OVER (PARTITION BY token, grp ORDER BY hour) AS median_price
, first_value(num_samples) OVER (PARTITION BY token, grp ORDER BY hour) AS sample_size

, symbol, decimals
     
FROM (
    SELECT 
    gs.hour, gs.token, gs.symbol, gs.decimals, p.median_price, p.num_samples, 
        count(p.median_price) OVER (PARTITION BY gs.token ORDER BY gs.hour) AS grp
    FROM hour_token_gs gs
    LEFT JOIN price p
        ON gs.hour = p.hour
        AND gs.token = p.token
        AND gs.symbol = p.symbol
        AND gs.decimals = p.decimals
    ) fill
)
,
rows AS (
    INSERT INTO prices.approx_prices_from_dex_data (
        contract_address,
        hour,
        median_price,
        sample_size,
        symbol,
        decimals
    )

    SELECT 
        contract_address,
        hour,
        median_price,
        sample_size,
        symbol,
        decimals
    FROM final_prices

    ON CONFLICT (contract_address, hour) DO UPDATE SET median_price = EXCLUDED.median_price, sample_size = EXCLUDED.sample_size
    RETURNING 1
)

SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- Monthly backfill starting 11 Nov 2021 (regenesis
--TODO: Add pre-regenesis prices

SELECT prices.insert_approx_prices_from_dex_data('2021-11-01', '2021-12-01')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2021-11-01' and hour < '2021-12-01');

SELECT prices.insert_approx_prices_from_dex_data('2021-12-01', '2021-12-14')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2021-12-01' and hour < '2021-12-14');

SELECT prices.insert_approx_prices_from_dex_data('2021-12-14', '2021-12-20')
WHERE NOT EXISTS (SELECT * FROM prices.approx_prices_from_dex_data WHERE hour >= '2021-12-14' and hour < '2021-12-20');

-- Have the insert script run twice every hour at minute 16 and 46
-- `start-time` is set to go back three days in time so that entries can be retroactively updated 
-- in case `dex.trades` or price data falls behind.
INSERT INTO cron.job (schedule, command)
VALUES ('16,46 * * * *', $$
    SELECT prices.insert_approx_prices_from_dex_data(
        (SELECT MAX(hour) - interval '1 hour' FROM prices.approx_prices_from_dex_data),
        (SELECT DATE_TRUNC('hour', now()) + interval '1 hour')
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;

--once per day run of the last 30 days to handle for new tokens
INSERT INTO cron.job (schedule, command)
VALUES ('1 0 * * *', $$
    SELECT prices.insert_approx_prices_from_dex_data(
        (SELECT MAX(hour) - interval '30 days' FROM prices.approx_prices_from_dex_data),
        (SELECT DATE_TRUNC('hour', now()) + interval '1 hour')
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
