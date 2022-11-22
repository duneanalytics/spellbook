CREATE OR REPLACE FUNCTION nft.insert_cryptopunks(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

-- update `PunkBought` event data in case of one of more punks bought via bid instead of direct list price purchase
WITH punks_union AS (
SELECT
    "evt_tx_hash",
    "evt_block_time",
    "evt_block_number",
    "evt_index",
    'erc20' AS erc_type,
    CAST("punkIndex" AS text) AS "tokenId",
    "fromAddress" AS "from",
    -- When the seller accepts a bid, the `PunkBought` event emits '\x0000000000000000000000000000000000000000' as `toAddress`
    -- Get the buyer from `erc20."ERC20_evt_Transfer"`
    CASE WHEN "toAddress" = '\x0000000000000000000000000000000000000000'
         THEN (SELECT "to"
               FROM erc20."ERC20_evt_Transfer" e
               WHERE e."evt_tx_hash" = p."evt_tx_hash" AND e."from" = p."fromAddress" ANd e.contract_address = '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb')
         ELSE "toAddress"
         END AS "to",
    "contract_address",
    -- When the seller accepts a bid, the `value` event emits '0'
    -- Get the value from the `PunkBidEntered` event but after double-checking that the last bidder equals the punk NFT recipient
    CASE WHEN "value" = 0
        THEN COALESCE(
        (
         SELECT value
         FROM cryptopunks."CryptoPunksMarket_evt_PunkBidEntered" b
         WHERE b."punkIndex" = p."punkIndex"
             AND b."fromAddress" = (
                SELECT "to"
                FROM erc20."ERC20_evt_Transfer" e
                WHERE e."evt_tx_hash" = p."evt_tx_hash" AND e."from" = p."fromAddress" AND e.contract_address = '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
                ORDER BY evt_block_time DESC
                LIMIT 1
         )
         AND evt_block_time <= p.evt_block_time
         ORDER BY evt_block_time DESC
         LIMIT 1
        )
        , 0)
        ELSE "value"
    END AS "value"
FROM cryptopunks."CryptoPunksMarket_evt_PunkBought" p
WHERE evt_block_time >= start_ts
AND evt_block_time < end_ts
),
-- aggregate all NFT transfers per transaction
punks_agg_tx AS (
SELECT
    evt_tx_hash,
    evt_block_time,
    evt_block_number,
    MIN(evt_index) AS evt_index,
    MAX(value) AS value,
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(value) AS erc1155_value_array
FROM punks_union
GROUP BY 1,2,3
),
rows AS (
    INSERT INTO nft.trades (
        block_time,
        nft_project_name,
        nft_token_id,
        erc_standard,
        platform,
        platform_version,
        trade_type,
        number_of_items,
        category,
        evt_type,
        usd_amount,
        seller,
        buyer,
        original_amount,
        original_amount_raw,
        original_currency,
        original_currency_contract,
        currency_contract,
        nft_contract_address,
        exchange_contract_address,
        tx_hash,
        block_number,
        nft_token_ids_array,
        senders_array,
        recipients_array,
        erc_types_array,
        nft_contract_addresses_array,
        erc_values_array,
        tx_from,
        tx_to,
        trace_address,
        evt_index,
        trade_id
    )

    SELECT
        trades.evt_block_time AS block_time,
        'CryptoPunks' AS nft_project_name,
        -- Set NFT token ID to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN trades.no_of_transfers > 1 THEN NULL ELSE CAST(trades.token_id_array[1] AS TEXT) END AS nft_token_id,
        'erc20' AS erc_standard,
        platform,
        platform_version,
        CASE WHEN trades.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        trades.no_of_transfers AS number_of_items,
        category,
        evt_type,
        trades.value / 10 ^ 18 * p.price AS usd_amount,
        trades.from_array[1] AS seller,
        trades.to_array[array_length(to_array, 1)] AS buyer,
        trades.value / 10 ^ 18 AS original_amount,
        trades.value AS original_amount_raw,
        'ETH' AS original_currency,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'::bytea AS nft_contract_address,
        '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'::bytea AS exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        -- Sometimes multiple NFT transfers occur in a given trade; the 'array' fields below provide info for these use cases
        trades.token_id_array AS nft_token_ids_array,
        trades.from_array AS senders_array,
        trades.to_array AS recipients_array,
        trades.erc_type_array AS erc_types_array,
        trades.contract_address_array AS nft_contract_addresses_array,
        trades.erc1155_value_array AS erc_values_array,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY platform, trades.evt_tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
        (
            SELECT
                'LarvaLabs Contract' AS platform,
                '1' AS platform_version,
                'Buy' AS category,
                'Trade' AS evt_type,
                *
            FROM punks_agg_tx) trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND p.minute >= start_ts
        AND p.minute < end_ts
    WHERE trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2017
SELECT nft.insert_cryptopunks(
    '2017-01-01',
    '2018-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2017-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2018-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2017-01-01'
    AND block_time <= '2018-01-01'
    AND platform = 'LarvaLabs Contract'
);

-- fill 2018
SELECT nft.insert_cryptopunks(
    '2018-01-01',
    '2019-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2018-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2019-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2018-01-01'
    AND block_time <= '2019-01-01'
    AND platform = 'LarvaLabs Contract'
);

-- fill 2019
SELECT nft.insert_cryptopunks(
    '2019-01-01',
    '2020-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2019-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2020-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2019-01-01'
    AND block_time <= '2020-01-01'
    AND platform = 'LarvaLabs Contract'
);


-- fill 2020
SELECT nft.insert_cryptopunks(
    '2020-01-01',
    '2021-01-01',
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2020-01-01'),
    (SELECT max(number) FROM ethereum.blocks WHERE time <= '2021-01-01')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2020-01-01'
    AND block_time <= '2021-01-01'
    AND platform = 'LarvaLabs Contract'
);

-- fill 2021
SELECT nft.insert_cryptopunks(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2021-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND platform = 'LarvaLabs Contract'
);

INSERT INTO cron.job (schedule, command)
VALUES ('53 * * * *', $$
    SELECT nft.insert_cryptopunks(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='LarvaLabs Contract'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='LarvaLabs Contract')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
