CREATE OR REPLACE FUNCTION perpetuals.insert_synthetix(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
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
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        evt_index,
        row_number() OVER (PARTITION BY project, tx_hash, evt_index ORDER BY version) AS trade_id
    FROM (
        --Kwenta/Synthetix
        SELECT
            s."evt_block_time" AS block_time,
            REPLACE(ENCODE(sm."asset", 'ESCAPE'), '\000', '') AS virtual_asset,
            SUBSTRING(REPLACE(ENCODE(sm."asset", 'ESCAPE'), '\000', ''), '[A-Z].*') AS underlying_asset,
            REPLACE(ENCODE(sm."marketKey", 'ESCAPE'), '\000', '') AS market,
            s."contract_address" AS market_address,
            ABS(s."tradeSize")/POW(10, 18) * p.price AS volume_usd,
            s."fee"/POW(10, 18) AS fee_usd,
            s."margin"/POW(10, 18) AS margin_usd,
            (ABS(s."tradeSize")/POW(10, 18) * p.price) / (s."margin"/POW(10, 18)) AS leverage_ratio,
            
            CASE
            WHEN (s."margin" >= 0 AND s."size" = 0 AND s."tradeSize" < 0 AND s."size" != s."tradeSize") THEN 'close' --closing long positions
            WHEN (s."margin" >= 0 AND s."size" = 0 AND s."tradeSize" > 0 AND s."size" != s."tradeSize") THEN 'close' --closing short positions
            WHEN s."tradeSize" > 0 THEN 'long'
            WHEN s."tradeSize" < 0 THEN 'short'
            ELSE 'NA'
            END AS "trade",
            
            'Synthetix' AS project,
            1 AS version,
            s."account" AS trader,
            s."tradeSize" AS volume_raw,
            s."evt_tx_hash" AS tx_hash,
            s."evt_index"
        FROM synthetix."FuturesMarket_evt_PositionModified" AS s
        LEFT JOIN synthetix."FuturesMarketManager_evt_MarketAdded" AS sm
            ON s."contract_address" = sm."market"
        LEFT JOIN (
            SELECT
                s."contract_address" AS market_address,
                REPLACE(ENCODE(sm."asset", 'ESCAPE'), '\000', '') AS asset,
                s."evt_block_time",
                AVG(s."lastPrice"/POW(10, 18)) AS price
            FROM synthetix."FuturesMarket_evt_PositionModified" AS s
            LEFT JOIN synthetix."FuturesMarketManager_evt_MarketAdded" AS sm
                ON s."contract_address" = sm."market"
            GROUP BY market_address, asset, s."evt_block_time"
            ) AS p
            ON s."contract_address" = p."market_address"
            AND s."evt_block_time" = p."evt_block_time"
        WHERE s."tradeSize" != 0
    ) AS perps
    INNER JOIN optimism."transactions" AS tx
        ON perps."tx_hash" = tx."hash"
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    
    --update if changes to trade classifications are needed
    ON CONFLICT (project, tx_hash, evt_index, trade_id)
    DO UPDATE SET
        trade = EXCLUDED.trade
    RETURNING 1
)

SELECT COUNT(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT perpetuals.insert_synthetix(
    '2021-11-10',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Synthetix' AND version = '1'
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT perpetuals.insert_synthetix(
        (SELECT MAX(block_time) - interval '1 days' FROM perpetuals.trades WHERE project='Synthetix' AND version = '1'),
        now()
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
