CREATE OR REPLACE FUNCTION nft.insert_foundation(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

-- Get ERC721 and ERC1155 transfer data for every trade transaction
WITH foundation_erc_union AS (
SELECT
    erc721.evt_tx_hash,
    erc721.evt_index,
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
AND erc721."from" <> '\x0000000000000000000000000000000000000000' -- exclude mints
UNION ALL
SELECT
    erc1155.evt_tx_hash,
    erc1155.evt_index,
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
AND erc1155."from" <> '\x0000000000000000000000000000000000000000' -- exclude mints
),
-- Aggregate NFT transfers per transaction 
foundation_erc_subsets AS (
SELECT
    evt_tx_hash,
    CASE WHEN erc_type = 'erc1155' THEN value
         WHEN erc_type = 'erc721'  THEN cardinality(array_agg(DISTINCT "tokenId")) END AS no_of_transfers,
    array_agg(DISTINCT erc_type) AS erc_type_array
FROM foundation_erc_union
GROUP BY 1,erc_type,value
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
        aggregator,
        usd_amount,
        seller,
        buyer,
        original_amount,
        original_amount_raw,
        eth_amount,
        royalty_fees_percent,
        original_royalty_fees,
        usd_royalty_fees,
        platform_fees_percent,
        original_platform_fees,
        usd_platform_fees,
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
        -- Set NFT token ID to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE CAST(created."tokenId" AS TEXT) END AS nft_token_id,
        -- Set ERC standard to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
        platform,
        platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        cast(trades.no_of_transfers as numeric) AS number_of_items,
        category,
        evt_type,
        agg.name AS aggregator,
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 * p.price AS usd_amount, --
        trades.seller, --
        trades.bidder AS buyer, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 AS original_amount, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") AS original_amount_raw, --
        (trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 AS eth_amount, --
        10 as  "royalty_fees_percent",
        ROUND(cast(10*((trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18)/100 as numeric),7) as original_royalty_fees,
        ROUND(cast(10*((trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 * p.price)/100 as numeric),7) as usd_royalty_fees,
        5 as "platform_fees_percent",
        ROUND(cast(5*((trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18)/100 as numeric),7) as original_platform_fees,
        ROUND(cast(5*((trades."f8nFee" + trades."ownerRev" + trades."creatorFee") / 10 ^ 18 * p.price)/100 as numeric),7) as usd_platform_fees,
        'ETH' AS original_currency,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        created."nftContract" AS nft_contract_address, -- Foundation NFT
        trades.contract_address AS exchange_contract_address, -- Foundation: Market
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
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
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN foundation_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
    LEFT JOIN foundation."market_evt_ReserveAuctionCreated" created ON trades."auctionId" = created."auctionId"
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = created."nftContract"
    LEFT JOIN nft.aggregators agg ON agg.contract_address = tx."to"
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

/*
INSERT INTO cron.job (schedule, command)
VALUES ('53 * * * *', $$
    SELECT nft.insert_foundation(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='Foundation'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='Foundation')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
