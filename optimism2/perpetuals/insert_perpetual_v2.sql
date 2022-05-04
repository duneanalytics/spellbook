CREATE OR REPLACE FUNCTION perpetuals.insert_perpetual_v2(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO perpetuals.trades (
        block_time,
        virtual_asset,
        underlying_asset,
        market,
        market_address,
        volume_usd,
        fee_usd,
        margin_usd,
        trade,
        project,
        version,
        trader,
        volume_raw,
        tx_hash,
        tx_from,
        tx_to,
        evt_index,
        trade_id
    )
    SELECT
        perps."block_time",
        COALESCE(e."symbol", CAST(perps."baseToken" AS TEXT)) AS virtual_asset,
        SUBSTRING(e."symbol", '[A-Z].*') AS underlying_asset,
        CONCAT(e."symbol", '-', 'USD') AS market,
        market_address,
        volume_usd,
        fee_usd,
        margin_usd,
        trade,
        project,
        version,
        trader,
        volume_raw,
        tx_hash,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index ORDER BY version) AS trade_id
    FROM (
        --Perpetual v2
        SELECT
            p."evt_block_time" AS block_time,
            p."baseToken",
            pp."pool" AS market_address,
            (ABS(p."exchangedPositionNotional") / POW(10, 18)) AS volume_usd,
            p."fee" / POW(10, 18) AS fee_usd,
            co."output_0" / POW(10, 6) AS margin_usd,
            
            CASE
            WHEN p."exchangedPositionSize" > 0 THEN 'long'
            WHEN p."exchangedPositionSize" < 0 THEN 'short'
            ELSE 'NA'
            END AS trade,
            
            'Perpetual' AS project,
            '2' AS version,
            p."trader",
            p."exchangedPositionNotional" AS volume_raw,
            p."evt_tx_hash" AS tx_hash,
            p."evt_index"
        FROM perp_v2."ClearingHouse_evt_PositionChanged" AS p
        LEFT JOIN perp_v2."Vault_call_getFreeCollateralByRatio" AS co
            ON p."evt_tx_hash" = co."call_tx_hash"
        LEFT JOIN perp_v2."MarketRegistry_evt_PoolAdded" AS pp
            ON p."baseToken" = pp."baseToken"
        WHERE co."call_success" = true
    ) AS perps
    LEFT JOIN erc20."tokens" AS e
        ON perps."baseToken" = e."contract_address"
    INNER JOIN optimism."transactions" AS tx
        ON perps."tx_hash" = tx."hash"
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    
    --update if we have updated info on old market addresses and erc20 table
    ON CONFLICT (project, tx_hash, evt_index, trade_id)
    DO UPDATE SET
        virtual_asset = EXCLUDED.virtual_asset,
        underlying_asset = EXCLUDED.underlying_asset,
        market = EXCLUDED.market,
        market_address = EXCLUDED.market_address,
        trade = EXCLUDED.trade
    RETURNING 1
)

SELECT COUNT(*) INTO r from rows;
RETURN r;
END
$function$;

-- due to the high amount of transactions on Perpetual, this table must be filled a month at a time from the Optimism 2.0 Regenesis
-- fill 2021-11-10 to 2021-12-10
SELECT perpetuals.insert_perpetual_v2(
    '2021-11-10',
    '2021-12-10',
    (SELECT MAX(number) FROM optimism.blocks WHERE time < '2021-11-10'),
    (SELECT MAX(number) FROM optimism.blocks WHERE time <= '2021-12-10')
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= '2021-12-10'
    AND project = 'Perpetual' AND version = '2'
);

-- fill 2021-12-10 to 2022-01-10
SELECT perpetuals.insert_perpetual_v2(
    '2021-12-10',
    '2022-01-10',
    (SELECT MAX(number) FROM optimism.blocks WHERE time < '2021-12-10'),
    (SELECT MAX(number) FROM optimism.blocks WHERE time <= '2022-01-10')
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2021-12-10'
    AND block_time <= '2022-01-10'
    AND project = 'Perpetual' AND version = '2'
);

-- fill 2022-01-10 to 2022-02-10
SELECT perpetuals.insert_perpetual_v2(
    '2022-01-10',
    '2022-02-10',
    (SELECT MAX(number) FROM optimism.blocks WHERE time < '2022-01-10'),
    (SELECT MAX(number) FROM optimism.blocks WHERE time <= '2022-02-10')
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2022-01-10'
    AND block_time <= '2022-02-10'
    AND project = 'Perpetual' AND version = '2'
);

-- fill 2022-02-10 to 2022-03-10
SELECT perpetuals.insert_perpetual_v2(
    '2022-02-10',
    '2022-03-10',
    (SELECT MAX(number) FROM optimism.blocks WHERE time < '2022-02-10'),
    (SELECT MAX(number) FROM optimism.blocks WHERE time <= '2022-03-10')
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2022-02-10'
    AND block_time <= '2022-03-10'
    AND project = 'Perpetual' AND version = '2'
);

-- fill 2022-03-10 to 2022-04-10
SELECT perpetuals.insert_perpetual_v2(
    '2022-03-10',
    '2022-04-10',
    (SELECT MAX(number) FROM optimism.blocks WHERE time < '2022-03-10'),
    (SELECT MAX(number) FROM optimism.blocks WHERE time <= '2022-04-10')
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2022-03-10'
    AND block_time <= '2022-04-10'
    AND project = 'Perpetual' AND version = '2'
);

-- fill 2022-04-10 onwards
SELECT perpetuals.insert_perpetual_v2(
    '2022-04-10',
    now(),
    (SELECT MAX(number) FROM optimism.blocks WHERE time < '2022-04-10'),
    (SELECT MAX(number) FROM optimism.blocks WHERE time <= now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2022-04-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Perpetual' AND version = '2'
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT perpetuals.insert_perpetual_v2(
        (SELECT MAX(block_time) - interval '1 days' FROM perpetuals.trades WHERE project = 'Perpetual' AND version = '2'),
        (SELECT now() - interval '20 minutes'),
        (SELECT MAX(number) FROM optimism.blocks WHERE time < (SELECT MAX(block_time) - interval '1 days' FROM perpetuals.trades WHERE project = 'Perpetual' AND version = '2')),
        (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
