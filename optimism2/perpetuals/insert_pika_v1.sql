CREATE OR REPLACE FUNCTION perpetuals.insert_pika_v1(start_ts timestamptz, end_ts timestamptz=now(), perpetuals_version integer=0) RETURNS integer
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
        -- Pika Protocol v1
        SELECT
            p."evt_block_time" AS block_time,
            
            CASE
            WHEN p."productId" = 1 THEN 'ETH'
            WHEN p."productId" = 2 THEN 'BTC'
            WHEN p."productId" = 3 THEN 'LINK'
            WHEN p."productId" = 4 THEN 'SNX'
            WHEN p."productId" = 5 THEN 'SOL'
            WHEN p."productId" = 6 THEN 'AVAX'
            WHEN p."productId" = 7 THEN 'MATIC'
            WHEN p."productId" = 8 THEN 'LUNA'
            WHEN p."productId" = 9 THEN 'AAVE'
            WHEN p."productId" = 10 THEN 'APE'
            WHEN p."productId" = 11 THEN 'AXS'
            WHEN p."productId" = 12 THEN 'UNI'
            ELSE CONCAT ('product_id_', p."productId") 
            END AS virtual_asset,
            
            CASE
            WHEN p."productId" = 1 THEN 'ETH'
            WHEN p."productId" = 2 THEN 'BTC'
            WHEN p."productId" = 3 THEN 'LINK'
            WHEN p."productId" = 4 THEN 'SNX'
            WHEN p."productId" = 5 THEN 'SOL'
            WHEN p."productId" = 6 THEN 'AVAX'
            WHEN p."productId" = 7 THEN 'MATIC'
            WHEN p."productId" = 8 THEN 'LUNA'
            WHEN p."productId" = 9 THEN 'AAVE'
            WHEN p."productId" = 10 THEN 'APE'
            WHEN p."productId" = 11 THEN 'AXS'
            WHEN p."productId" = 12 THEN 'UNI'
            ELSE CONCAT ('product_id_', p."productId") 
            END AS underlying_asset,
            
            CASE
            WHEN p."productId" = 1 THEN 'ETH-USD'
            WHEN p."productId" = 2 THEN 'BTC-USD'
            WHEN p."productId" = 3 THEN 'LINK-USD'
            WHEN p."productId" = 4 THEN 'SNX-USD'
            WHEN p."productId" = 5 THEN 'SOL-USD'
            WHEN p."productId" = 6 THEN 'AVAX-USD'
            WHEN p."productId" = 7 THEN 'MATIC-USD'
            WHEN p."productId" = 8 THEN 'LUNA-USD'
            WHEN p."productId" = 9 THEN 'AAVE-USD'
            WHEN p."productId" = 10 THEN 'APE-USD'
            WHEN p."productId" = 11 THEN 'AXS-USD'
            WHEN p."productId" = 12 THEN 'UNI-USD'
            ELSE CONCAT ('product_id_', p."productId") 
            END AS market,
            
            p."contract_address" AS market_address,
            (p."margin"/POW(10, 8)) * (p."leverage"/POW(10, 8)) AS volume_usd,
            p."fee"/POW(10, 8) AS fee_usd,
            p."margin"/POW(10,8) AS margin_usd,
            
            CASE
            WHEN p."isLong" = 'true' THEN 'long'
            WHEN p."isLong" = 'false' THEN 'short'
            ELSE p."isLong"
            END AS trade,
            
            'Pika' AS project,
            p."version",
            p."user" AS trader,
            p."margin" * p."leverage" AS volume_raw,
            p."evt_tx_hash" AS tx_hash,
            p."evt_index"
        FROM (
                SELECT
                    "positionId",
                    "user",
                    "productId",
                    CAST("isLong" AS TEXT),
                    "price",
                    "oraclePrice",
                    "margin",
                    "leverage",
                    0 AS "fee",
                    "contract_address",
                    "evt_tx_hash",
                    "evt_index",
                    "evt_block_time",
                    "evt_block_number",
                    '1' AS version
                FROM pika_perp."PikaPerpV2_evt_NewPosition"
        
                UNION ALL
                --closing positions
                SELECT
                    "positionId",
                    "user",
                    "productId",
                    'close' AS "action",
                    "price",
                    "entryPrice",
                    "margin",
                    "leverage",
                    0 AS fee,
                    "contract_address",
                    "evt_tx_hash",
                    "evt_index",
                    "evt_block_time",
                    "evt_block_number",
                    '1' AS version
                FROM pika_perp."PikaPerpV2_evt_ClosePosition"
            ) AS p
    ) AS perps
    INNER JOIN optimism."transactions" AS tx
        ON perps."tx_hash" = tx."hash"
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        
    --update if we have updated info on product_ids come in
    ON CONFLICT (project, tx_hash, evt_index, trade_id)
    DO UPDATE SET
        virtual_asset = EXCLUDED.virtual_asset,
        underlying_asset = EXCLUDED.underlying_asset,
        market = EXCLUDED.market,
        trade = EXCLUDED.trade
    RETURNING 1
)

SELECT COUNT(*) INTO r from rows;
RETURN r;
END
$function$;

SELECT perpetuals.insert_pika_v1(
    '2021-11-10',
    now(),
    1
)
WHERE NOT EXISTS (
    SELECT *
    FROM perpetuals.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Pika' AND version = '1'
);

INSERT INTO cron.job (schedule, command)
VALUES ('15,45 * * * *', $$
    SELECT perpetuals.insert_pika_v1(
        (SELECT MAX(block_time) - interval '1 days' FROM perpetuals.trades WHERE project='Pika' AND version = '1'),
        now()
    );
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
