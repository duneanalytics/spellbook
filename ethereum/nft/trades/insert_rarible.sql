CREATE OR REPLACE FUNCTION nft.insert_rarible(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH all_data AS (
-- Get data from various Rarible contracts deployed over time
-- Oct 2019 fading out in Summer 2020
    SELECT
        'Rarible' as platform,
        '1' as platform_version,
        '\xf2ee97405593bc7b6275682b0331169a48fedec7'::bytea AS exchange_contract_address,
        'Trade' as evt_type,
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token AS nft_contract_address,
        CAST("tokenId" AS TEXT) AS nft_token_id,
        seller,
        buyer,
        price AS original_amount_raw, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
        'TokenSale_evt_Buy' as category
     FROM rarible."TokenSale_evt_Buy" 
    UNION ALL
-- from May 2020 to Sep 2020
    SELECT 
        'Rarible',
        '1',
        '\x8c530a698b6e83d562db09079bc458d4dad4e6c5',
        'Trade',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        CAST("tokenId" AS TEXT),
        owner,
        buyer,
        price*value, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC1155Sale_v1_evt_Buy'
    FROM rarible_v1."ERC1155Sale_v1_evt_Buy"
    UNION ALL
-- from May 2020 to Sep 2020
    SELECT
        'Rarible',
        '1',
        '\xa5af48b105ddf2fa73cbaac61d420ea31b3c2a07',
        'Trade',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        CAST("tokenId" AS TEXT),
        seller,
        buyer,
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC721Sale_v1_evt_Buy'
    FROM rarible_v1."ERC721Sale_v1_evt_Buy"
    UNION ALL
-- from Sep 2020 and fading around end of 2020
    SELECT
        'Rarible',
        '1',
        '\x131aebbfe55bca0c9eaad4ea24d386c5c082dd58',
        'Trade',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        CAST("tokenId" AS TEXT),
        seller,
        buyer,
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC721Sale_v2_evt_Buy'
    FROM rarible_v1."ERC721Sale_v2_evt_Buy"
    UNION ALL
-- from Sep 2020 and fading around end of 2020
    SELECT
        'Rarible',
        '1',
        '\x93f2a75d771628856f37f256da95e99ea28aafbe',
        'Trade',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        token,
        CAST("tokenId" AS TEXT),
        owner,
        buyer,
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea,
        'ERC1155Sale_v2_evt_Buy'
    FROM rarible_v1."ERC1155Sale_v2_evt_Buy"
    UNION ALL
-- from Nov 2020 ongoing
    SELECT
        'Rarible',
        '1',
        '\xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
        'Trade',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        "sellToken",
        CAST("sellTokenId" AS TEXT),
        owner,
        buyer,
        "buyValue" * amount / "sellValue",
        "buyToken",
        CASE
            WHEN "buyToken" = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE "buyToken"
        END, 
        'ExchangeV1_evt_Buy' as category
    FROM rarible."ExchangeV1_evt_Buy"
    where "buyTokenId" = 0 --buy
    UNION ALL
-- from Nov 2020 ongoing
    SELECT
        'Rarible',
        '1',
        '\xcd4ec7b66fbc029c116ba9ffb3e59351c20b5b06',
        'Trade',
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        "buyToken",
        CAST( "buyTokenId" AS TEXT),
        buyer AS seller,
        owner AS buyer,
        amount,
        "sellToken", 
        CASE
            WHEN "sellToken" = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE "sellToken"
        END, -- currency_contract,
        'ExchangeV1_evt_Buy' as category
    FROM rarible."ExchangeV1_evt_Buy"
    where "sellTokenId" = 0 
    UNION ALL
-- from 2021-06-15 onwards
-- 'Purchases' in ETH
    SELECT
        'Rarible' as platform,
        '2' as platform_version,
        contract_address AS exchange_contract_address,
        'Trade' as evt_type,
        tx_hash AS evt_tx_hash,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        "index" AS evt_index,
        substring(data FROM 365 FOR 20) AS nft_contract_address,
        CAST(bytea2numericpy(substring(data FROM 385 FOR 32)) AS TEXT) AS nft_token_id,
        substring(data FROM 77 FOR 20) AS seller,
        substring(data FROM 109 FOR 20) AS buyer,
        bytea2numericpy(substring(data FROM 129 FOR 32)) original_amount_raw,
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
        'Buy' AS category -- 'Purchase'
    FROM ethereum."logs" 
    WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6' 
    AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
    AND length(data) = 512
UNION ALL
-- from 2021-06-15 onwards
-- 'Bid Accepted' non-ETH
    SELECT
        'Rarible' as platform,
        '2' as platform_version,
        contract_address AS exchange_contract_address,
        'Trade' as evt_type,
         tx_hash AS evt_tx_hash,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        "index" AS evt_index,
        substring(data FROM 493 FOR 20) AS nft_contract_address,
        CAST(bytea2numericpy(substring(data FROM 513 FOR 32)) AS TEXT) AS nft_token_id,
        substring(data FROM 109 FOR 20) AS seller,
        substring(data FROM 77 FOR 20) AS buyer,
        bytea2numericpy(substring(data FROM 161 FOR 32)) AS original_amount_raw,
        substring(data FROM 365 FOR 20) AS original_currency_contract,
        substring(data FROM 365 FOR 20) AS currency_contract,
        'Offer Accepted' AS category -- 'Bid Accepted'
    FROM ethereum."logs"
    WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6'
    AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
    AND length(data) = 544
    AND bytea2numericpy(substring(data FROM 225 FOR 32)) = 384
UNION ALL
-- from 2021-06-15 onwards
-- 'Purchases' in non-ETH currencies
    SELECT
        'Rarible' as platform,
        '2' as platform_version,
        contract_address AS exchange_contract_address,
        'Trade' as evt_type,
        tx_hash AS evt_tx_hash,
        block_time AS evt_block_time,
        block_number AS evt_block_number,
        "index" AS evt_index,
        substring(data FROM 365 FOR 20) AS nft_contract_address,
        CAST(bytea2numericpy(substring(data FROM 385 FOR 32)) AS TEXT) AS nft_token_id,
        substring(data FROM 77 FOR 20) AS seller,
        substring(data FROM 109 FOR 20) AS buyer,
        bytea2numericpy(substring(data FROM 129 FOR 32)) AS original_amount_raw,
        substring(data FROM 525 FOR 20) AS original_currency_contract,
        substring(data FROM 525 FOR 20) AS currency_contract,
        'Buy' AS category -- 'Bid Accepted'
    FROM ethereum."logs"
    WHERE "contract_address" = '\x9757f2d2b135150bbeb65308d4a91804107cd8d6'
    AND topic1 = '\x268820db288a211986b26a8fda86b1e0046281b21206936bb0e61c67b5c79ef4'
    AND length(data) = 544
    AND bytea2numericpy(substring(data FROM 225 FOR 32)) = 416
),
rows AS (
    INSERT INTO nft.trades (
	block_time,
	nft_project_name,
	nft_token_id,
	platform,
	platform_version,
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
	tx_from,
	tx_to,
	trace_address,
	evt_index,
	trade_id
    )

    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        trades.nft_token_id,
        trades.platform,
        trades.platform_version,
        trades.category,
        trades.evt_type,
        trades.original_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
        trades.seller,
        trades.buyer,
        trades.original_amount_raw / 10 ^ erc20.decimals AS original_amount,
        trades.original_amount_raw AS original_amount_raw,
        CASE WHEN trades.original_currency_contract = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        trades.original_currency_contract,
        trades.currency_contract,
        trades.nft_contract_address,
        trades.exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY platform, trades.evt_tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
        all_data trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_contract
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = trades.currency_contract
        AND p.minute >= start_ts
        AND p.minute < end_ts
    WHERE
        trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
   ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2019
SELECT nft.insert_rarible(
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
    AND platform = 'Rarible'
);


-- fill 2020
SELECT nft.insert_rarible(
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
    AND platform = 'Rarible'
);

-- fill 2021
SELECT nft.insert_rarible(
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
    AND platform = 'Rarible'
);

INSERT INTO cron.job (schedule, command)
VALUES ('53 * * * *', $$
    SELECT nft.insert_rarible(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='Rarible'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='Rarible')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
