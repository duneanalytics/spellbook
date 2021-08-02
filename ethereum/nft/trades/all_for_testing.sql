-- CREATE OR REPLACE FUNCTION nft.insert_cryptopunks(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
WITH punks_union AS (
SELECT
    "evt_tx_hash",
    "evt_block_time",
    "evt_block_number",
    "evt_index",
    'erc20' AS erc_type,
    CAST("punkIndex" AS text) AS "tokenId",
    "fromAddress" AS "from",
    CASE WHEN "toAddress" = '\x0000000000000000000000000000000000000000'
         THEN (SELECT "to"
               FROM erc20."ERC20_evt_Transfer" e
               WHERE e."evt_tx_hash" = p."evt_tx_hash" AND e."from" = p."fromAddress" ANd e.contract_address = '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb')
         ELSE "toAddress"
         END AS "to",
    "contract_address",
    CASE WHEN "value" = 0
         THEN COALESCE(
             (
                 SELECT value
                 FROM cryptopunks."CryptoPunksMarket_evt_PunkBidEntered" b
                 WHERE b."punkIndex" = p."punkIndex"
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
punks_final AS (
    SELECT
        trades.evt_block_time AS block_time,
        'CryptoPunks' AS nft_project_name,
        CASE WHEN trades.no_of_transfers > 1 THEN NULL ELSE CAST(trades.token_id_array[1] AS TEXT) END AS nft_token_id, -- modified
        'erc20' AS erc_standard, -- new
        platform,
        platform_version,
        CASE WHEN trades.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type, -- new
        trades.no_of_transfers AS number_of_items, -- new
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
        trades.token_id_array AS nft_token_ids_array, -- new
        trades.from_array AS senders_array, -- new
        trades.to_array AS recipients_array, -- new
        trades.erc_type_array AS erc_types_array, -- new
        trades.contract_address_array AS nft_contract_addresses_array, -- new
        trades.erc1155_value_array AS erc_values_array, -- new
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
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND p.minute >= start_ts
        AND p.minute < end_ts
    WHERE trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
)



-- CREATE OR REPLACE FUNCTION nft.insert_foundation(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
foundation_erc_union AS (
SELECT
    erc721.evt_tx_hash,
    'erc721' as erc_type,
    CAST(erc721."tokenId" AS TEXT) AS "tokenId",
    erc721."from",
    erc721."to",
    erc721.contract_address,
    NULL::numeric AS value
FROM erc721."ERC721_evt_Transfer" erc721
INNER JOIN foundation."market_evt_ReserveAuctionFinalized" f ON erc721.evt_tx_hash = f.evt_tx_hash
WHERE erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
AND erc721."from" <> '\x0000000000000000000000000000000000000000'
UNION ALL
SELECT
    erc1155.evt_tx_hash,
    'erc1155' as erc_type,
    CAST(erc1155.id AS TEXT) AS "tokenId",
    erc1155."from",
    erc1155."to",
    erc1155.contract_address,
    erc1155.value
FROM erc1155."ERC1155_evt_TransferSingle" erc1155
INNER JOIN foundation."market_evt_ReserveAuctionFinalized" f ON erc1155.evt_tx_hash = f.evt_tx_hash
WHERE erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
AND erc1155."from" <> '\x0000000000000000000000000000000000000000'
),
foundation_erc_subsets AS (
SELECT
    evt_tx_hash,
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(value) AS erc1155_value_array
FROM foundation_erc_union
GROUP BY 1
),
foundation_final AS (
    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE CAST(created."tokenId" AS TEXT) END AS nft_token_id, -- modified
        tokens.standard AS erc_standard, -- new
        platform,
        platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type, -- new
        erc.no_of_transfers AS number_of_items, -- new
        category,
        evt_type,
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 * p.price AS usd_amount, --
        trades.seller, --
        trades.bidder AS buyer, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 AS original_amount, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") AS original_amount_raw, --
        'ETH' AS original_currency,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        created."nftContract" AS nft_contract_address, -- Foundation NFT
        trades.contract_address AS exchange_contract_address, -- Foundation: Market
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        erc.token_id_array AS nft_token_ids_array, -- new
        erc.from_array AS senders_array, -- new
        erc.to_array AS recipients_array, -- new
        erc.erc_type_array AS erc_types_array, -- new
        erc.contract_address_array AS nft_contract_addresses_array, -- new
        erc.erc1155_value_array AS erc_values_array, -- new
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY platform, trades.evt_tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
        (SELECT 
            'Foundation' AS platform,
            '1' AS platform_version,
            'Auction Settled' AS category,
            'Trade' AS evt_type,
            *
        FROM foundation."market_evt_ReserveAuctionFinalized") trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN foundation_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
    LEFT JOIN foundation."market_evt_ReserveAuctionCreated" created ON trades."auctionId" = created."auctionId"
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = created."nftContract"
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND p.minute >= start_ts
        AND p.minute < end_ts
    WHERE trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
)



-- CREATE OR REPLACE FUNCTION nft.insert_opensea(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
wyvern_calldata AS (
    SELECT
        'OpenSea' AS platform,
        '1' AS platform_version,
        'Buy' AS category,
        'Trade' AS evt_type,
        call_tx_hash,
        addrs [5] AS nft_contract_address,
        addrs [2] AS buyer,
        addrs [9] AS seller,
        addrs [7] AS original_currency_address,
        CASE
            WHEN addrs [7] = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE addrs [7]
        END AS currency_token,
        CAST(
            bytea2numericpy(
                substring(
                    "calldataBuy"
                    FROM
                        69 FOR 32
                )
            ) AS TEXT
        ) AS token_id,
        call_trace_address
    FROM
        opensea."WyvernExchange_call_atomicMatch_"
    WHERE
        "call_success"
),
opensea_erc_union AS (
SELECT
    erc721.evt_tx_hash,
    'erc721' as erc_type,
    CAST(erc721."tokenId" AS TEXT) AS "tokenId",
    erc721."from",
    erc721."to",
    erc721.contract_address,
    NULL::numeric AS value
FROM erc721."ERC721_evt_Transfer" erc721
INNER JOIN wyvern_calldata wc ON erc721.evt_tx_hash = wc.call_tx_hash
WHERE erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
AND erc721."from" <> '\x0000000000000000000000000000000000000000'
UNION ALL
SELECT
    erc1155.evt_tx_hash,
    'erc1155' as erc_type,
    CAST(erc1155.id AS TEXT) AS "tokenId",
    erc1155."from",
    erc1155."to",
    erc1155.contract_address,
    erc1155.value
FROM erc1155."ERC1155_evt_TransferSingle" erc1155
INNER JOIN wyvern_calldata wc ON erc1155.evt_tx_hash = wc.call_tx_hash
WHERE erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
AND erc1155."from" <> '\x0000000000000000000000000000000000000000'
),
opensea_erc_subsets AS (
SELECT
    evt_tx_hash,
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(value) AS erc1155_value_array
FROM opensea_erc_union
GROUP BY 1
),
opensea_final AS (
    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE token_id END AS nft_token_id, -- modified
        tokens.standard AS erc_standard, -- new
        wc.platform,
        wc.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type, -- new
        erc.no_of_transfers AS number_of_items, -- new
        wc.category,
        wc.evt_type,
        trades.price / 10 ^ erc20.decimals * p.price AS usd_amount,
        wc.seller,
        wc.buyer,
        trades.price / 10 ^ erc20.decimals AS original_amount,
        trades.price AS original_amount_raw,
        CASE WHEN wc.original_currency_address = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        wc.original_currency_address AS original_currency_contract,
        wc.currency_token AS currency_contract,
        COALESCE(erc.contract_address_array[1], wc.nft_contract_address) AS nft_contract_address, -- modified
        trades.contract_address AS exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number,
        erc.token_id_array AS nft_token_ids_array, -- new
        erc.from_array AS senders_array, -- new
        erc.to_array AS recipients_array, -- new
        erc.erc_type_array AS erc_types_array, -- new
        erc.contract_address_array AS nft_contract_addresses_array, -- new
        erc.erc1155_value_array AS erc_values_array, -- new
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        call_trace_address AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY wc.platform, trades.evt_tx_hash, trades.evt_index, wc.category ORDER BY wc.platform_version, wc.evt_type) AS trade_id
    FROM
        opensea."WyvernExchange_evt_OrdersMatched" trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN opensea_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = trades.evt_tx_hash
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = wc.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = wc.currency_token
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
    WHERE
        NOT EXISTS (SELECT *
            FROM erc721."ERC721_evt_Transfer" erc721
            WHERE trades.evt_tx_hash = erc721.evt_tx_hash
            AND erc721."from" = '\x0000000000000000000000000000000000000000')
        AND trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
)



-- CREATE OR REPLACE FUNCTION nft.insert_rarible(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
rarible_trades AS (
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
    where "buyTokenId" = 0 AND "sellTokenId" <> 0 
 --buy
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
rarible_erc_union AS (
SELECT
    erc721.evt_tx_hash,
    'erc721' as erc_type,
    CAST(erc721."tokenId" AS TEXT) AS "tokenId",
    erc721."from",
    erc721."to",
    erc721.contract_address,
    NULL::numeric AS value
FROM erc721."ERC721_evt_Transfer" erc721
INNER JOIN rarible_trades d ON erc721.evt_tx_hash = d.evt_tx_hash
WHERE erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
AND erc721."from" <> '\x0000000000000000000000000000000000000000'
UNION ALL
SELECT
    erc1155.evt_tx_hash,
    'erc1155' as erc_type,
    CAST(erc1155.id AS TEXT) AS "tokenId",
    erc1155."from",
    erc1155."to",
    erc1155.contract_address,
    erc1155.value
FROM erc1155."ERC1155_evt_TransferSingle" erc1155
INNER JOIN rarible_trades d ON erc1155.evt_tx_hash = d.evt_tx_hash
WHERE erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
AND erc1155."from" <> '\x0000000000000000000000000000000000000000'
),
rarible_erc_subsets AS (
SELECT
    evt_tx_hash,
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(value) AS erc1155_value_array
FROM rarible_erc_union
GROUP BY 1
),
rarible_final AS (
    SELECT
        trades.evt_block_time AS block_time,
        tokens.name AS nft_project_name,
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE trades.nft_token_id END AS nft_token_id, -- modified
        tokens.standard AS erc_standard, -- new
        trades.platform,
        trades.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type, -- new
        erc.no_of_transfers AS number_of_items, -- new
        trades.category,
        trades.evt_type,
        trades.original_amount_raw / 10 ^ erc20.decimals * p.price AS usd_amount,
        COALESCE(erc.from_array[1], trades.seller) AS seller, -- modified
        COALESCE(erc.to_array[1], trades.buyer) AS buyer, -- modified
        trades.original_amount_raw / 10 ^ erc20.decimals AS original_amount,
        trades.original_amount_raw AS original_amount_raw,
        CASE WHEN trades.original_currency_contract = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        trades.original_currency_contract,
        trades.currency_contract,
        trades.nft_contract_address,
        trades.exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
        erc.token_id_array AS nft_token_ids_array, -- new
        erc.from_array AS senders_array, -- new
        erc.to_array AS recipients_array, -- new
        erc.erc_type_array AS erc_types_array, -- new
        erc.contract_address_array AS nft_contract_addresses_array, -- new
        erc.erc1155_value_array AS erc_values_array, -- new
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY platform, trades.evt_tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
        rarible_trades trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN rarible_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_contract
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = trades.currency_contract
        AND p.minute >= start_ts
        AND p.minute < end_ts
    WHERE
        trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
)



-- CREATE OR REPLACE FUNCTION nft.insert_superrare(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
superrare_trades AS (
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as BYTEA) as "from",
        CAST(substring(topic4 FROM 13) as BYTEA) as "to",
        bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
        '2' as platform_version,
        contract_address AS exchange_contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        'Buy' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND
        topic1 = '\x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'
UNION ALL
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
        CAST(substring(topic2 FROM 13) as BYTEA) as "from",
        CAST(substring(topic3 FROM 13) as BYTEA) as "to",
        bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
        '1' as platform_version,
        contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        'Buy' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\x16dd16959a056953a63cf14bf427881e762e54f03d86b864efea8238dd3b822f'
UNION ALL
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as BYTEA) as "from",
        CAST(substring(topic4 FROM 13) as BYTEA) as "to",
        bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
        '2' as platform_version,
        contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        'Buy' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND
        topic1 = '\x5764dbcef91eb6f946584f4ea671217c686fa7e858ce4f9f42d08422b86556a9'
UNION ALL
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as BYTEA) as "from",
        CAST(substring(topic4 FROM 13) as BYTEA) as "to",
        bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
        '2' as platform_version,
        contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        'Offer Accepted' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x2947f98c42597966a0ec25e92843c09ac17fbaa7'
    AND
        topic1 = '\x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6'
UNION ALL
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
        CAST(substring(topic2 FROM 13) as BYTEA) as "from",
        CAST(substring(topic3 FROM 13) as BYTEA) as "to",
        bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
        '1' as platform_version,
        contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        'Offer Accepted' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
    AND
        topic1 = '\xd6deddb2e105b46d4644d24aac8c58493a0f107e7973b2fe8d8fa7931a2912be'
UNION ALL
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as BYTEA) as "from",
        CAST(substring(topic4 FROM 13) as BYTEA) as "to",
        bytea2numericpy(substring(data FOR 32)) AS original_amount_raw,
        '2' as platform_version,
        contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        'Offer Accepted' AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x65b49f7aee40347f5a90b714be4ef086f3fe5e2c'
    AND
        topic1 = '\x2a9d06eec42acd217a17785dbec90b8b4f01a93ecd8c127edd36bfccf239f8b6'
UNION ALL
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(topic4) as TEXT) AS token_id,
        CAST(substring(topic3 FROM 13) as BYTEA) as "from",
        CAST(substring(data FROM 13 FOR 20) as BYTEA) as "to",
        bytea2numericpy(substring(data FROM 33 FOR 32)) AS original_amount_raw,
        '2' as platform_version,
        contract_address,
        block_number,
        "index" AS evt_index,
	'Trade' as evt_type,
        CASE WHEN topic3 = '\x0000000000000000000000000000000000000000000000000000000000000000' THEN 'Auction Retired' ELSE 'Auction Settled' END AS category
    FROM
        ethereum."logs"
    WHERE
        contract_address = '\x8c9f364bf7a56ed058fc63ef81c6cf09c833e656'
    AND
        topic1 = '\xea6d16c6bfcad11577aef5cc6728231c9f069ac78393828f8ca96847405902a9'
), 
superrare_erc_union AS (
SELECT
    erc721.evt_tx_hash,
    'erc721' as erc_type,
    CAST(erc721."tokenId" AS TEXT) AS "tokenId",
    erc721."from",
    erc721."to",
    erc721.contract_address,
    NULL::numeric AS value
FROM erc721."ERC721_evt_Transfer" erc721
INNER JOIN superrare_trades ON superrare_trades.tx_hash = erc721.evt_tx_hash
WHERE erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
AND erc721."from" <> '\x0000000000000000000000000000000000000000'
UNION ALL
SELECT
    erc1155.evt_tx_hash,
    'erc1155' as erc_type,
    CAST(erc1155.id AS TEXT) AS "tokenId",
    erc1155."from",
    erc1155."to",
    erc1155.contract_address,
    erc1155.value
FROM erc1155."ERC1155_evt_TransferSingle" erc1155
INNER JOIN superrare_trades ON superrare_trades.tx_hash = erc1155.evt_tx_hash
WHERE erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
AND erc1155."from" <> '\x0000000000000000000000000000000000000000'
UNION ALL 
SELECT
    erc20.evt_tx_hash,
    'erc20' as erc_type,
    NULL::text AS "tokenId",
    erc20."from",
    erc20."to",
    erc20.contract_address,
    erc20.value
FROM erc20."ERC20_evt_Transfer" erc20
INNER JOIN superrare_trades ON superrare_trades.tx_hash = erc20.evt_tx_hash
WHERE erc20.evt_block_time >= start_ts
AND erc20.evt_block_time < end_ts
AND erc20."from" <> '\x0000000000000000000000000000000000000000'
AND erc20.contract_address = '\xb47e3cd837ddf8e4c57f05d70ab865de6e193bbb'
UNION ALL
SELECT
    st.evt_tx_hash,
    'SupeRare' as erc_type,
    CAST("_tokenId" AS TEXT) AS "tokenId",
    st."_from",
    st."_to",
    st.contract_address,
    NULL::numeric AS value
FROM superrare."SuperRare_evt_Transfer" st
INNER JOIN superrare_trades ON superrare_trades.tx_hash = st.evt_tx_hash
WHERE st.evt_block_time >= '2019-01-01'
AND st.evt_block_time < now()
AND st."_from" <> '\x0000000000000000000000000000000000000000'
AND st.contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
),
superrare_erc_subsets AS (
SELECT
    evt_tx_hash,
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(value) AS erc1155_value_array
FROM superrare_erc_union
GROUP BY 1
),
superrare_final AS (
    SELECT
	trades.block_time,
        tokens.name AS nft_project_name,
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE trades.token_id END AS nft_token_id, -- modified
        tokens.standard AS erc_standard, -- new
	trades.platform,
	trades.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type, -- new
        erc.no_of_transfers AS number_of_items, -- new
	category,
	evt_type,
	trades.original_amount_raw / 10^18 * p.price AS usd_amount,
	erc.from_array[1] AS seller,
	erc.to_array[1] AS buyer,
	trades.original_amount_raw / 10^18 as original_amount,
	trades.original_amount_raw as original_amount_raw,
	'ETH' AS original_currency,
	'\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract, 
	'\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
	erc.contract_address_array[1] AS nft_contract_address,
	trades.exchange_contract_address,
	trades.tx_hash,
	trades.block_number,
        erc.token_id_array AS nft_token_ids_array, -- new
        erc.from_array AS senders_array, -- new
        erc.to_array AS recipients_array, -- new
        erc.erc_type_array AS erc_types_array, -- new
        erc.contract_address_array AS nft_contract_addresses_array, -- new
        erc.erc1155_value_array AS erc_values_array, -- new
	tx."from" AS tx_from,
	tx."to" AS tx_to,
	NULL::integer[] AS trace_address,
	trades.evt_index,
        row_number() OVER (PARTITION BY platform, trades.tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
	superrare_trades trades
    INNER JOIN ethereum.transactions tx
        ON trades.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
    LEFT JOIN superrare_erc_subsets erc ON erc.evt_tx_hash = trades.tx_hash
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = erc.contract_address_array[1]
    WHERE category IN ('Buy','Offer Accepted','Auction Settled')
    AND trades.block_time >= start_ts
    AND trades.block_time < end_ts
)
