CREATE OR REPLACE FUNCTION nft.insert_rarible(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH rarible_trades AS (
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
        price, -- including decimals
        '\x0000000000000000000000000000000000000000'::bytea as original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea as currency_contract,
        'TokenSale_evt_Buy' as category
     FROM rarible."TokenSale_evt_Buy" 
     WHERE evt_block_time >= start_ts
     AND evt_block_time < end_ts
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
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts
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
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts
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
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts
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
    WHERE evt_block_time >= start_ts
    AND evt_block_time < end_ts
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
    AND evt_block_time >= start_ts
    AND evt_block_time < end_ts
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
    AND evt_block_time >= start_ts
    AND evt_block_time < end_ts
    UNION ALL
-- from 2021-06-15 onwards
    -- ETH purchases of ERC1155 or ERC721 
    SELECT
        'Rarible' AS platform,
        '2' AS platform_version,
        contract_address AS exchange_contract_address,
        'Trade' AS evt_type,
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        decode(substring("leftAsset"->>'data' FROM 27 FOR 40), 'hex') AS nft_contract_address,
        CAST(bytea2numericpy(decode(substring("leftAsset"->>'data' FROM 67 FOR 64), 'hex')) AS TEXT) AS nft_token_id,
        "leftMaker" AS seller,
        "rightMaker" AS buyer,
        "newLeftFill" AS original_amount_raw,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        'Buy' AS category -- 'Purchase'
    FROM rarible."ExchangeV2_evt_Match"
    WHERE "rightAsset"->>'assetClass' = '0xaaaebeba' -- ETH
    AND (
        "leftAsset"->>'assetClass' = '0x973bb640' 
        OR 
        "leftAsset"->>'assetClass' = '0x73ad2146'
        )
    AND evt_block_time >= start_ts
    AND evt_block_time < end_ts
    UNION ALL
    -- ERC20 purchases of ERC1155 or ERC721 
    SELECT
        'Rarible' AS platform,
        '2' AS platform_version,
        contract_address AS exchange_contract_address,
        'Trade' AS evt_type,
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        decode(substring("leftAsset"->>'data' FROM 27 FOR 40), 'hex') AS nft_contract_address,
        CAST(bytea2numericpy(decode(substring("leftAsset"->>'data' FROM 67 FOR 64), 'hex')) AS TEXT) AS nft_token_id,
        "leftMaker" AS seller,
        "rightMaker" AS buyer,
        "newLeftFill" AS original_amount_raw,
        decode(substring("rightAsset"->>'data' FROM 27 FOR 40), 'hex') AS original_currency_contract,
        decode(substring("rightAsset"->>'data' FROM 27 FOR 40), 'hex') AS currency_contract,
        'Buy' AS category -- 'Purchase'
    FROM rarible."ExchangeV2_evt_Match"
    WHERE "rightAsset"->>'assetClass' = '0x8ae85d84'
    AND (
        "leftAsset"->>'assetClass' = '0x973bb640' 
        OR
        "leftAsset"->>'assetClass' = '0x73ad2146' 
        )
    AND evt_block_time >= start_ts
    AND evt_block_time < end_ts
    UNION ALL
    -- WETH Bid Accepted
    SELECT
        'Rarible' AS platform,
        '2' AS platform_version,
        contract_address AS exchange_contract_address,
        'Trade' AS evt_type,
        evt_tx_hash,
        evt_block_time,
        evt_block_number,
        evt_index,
        decode(substring("rightAsset"->>'data' FROM 27 FOR 40), 'hex') AS nft_contract_address,
        CAST(bytea2numericpy(decode(substring("rightAsset"->>'data' FROM 67 FOR 64), 'hex')) AS TEXT) AS nft_token_id,
        "rightMaker" AS seller,
        "leftMaker" AS buyer,
        "newRightFill" AS original_amount_raw,
        decode(substring("leftAsset"->>'data' FROM 27 FOR 40), 'hex') AS original_currency_contract,
        decode(substring("leftAsset"->>'data' FROM 27 FOR 40), 'hex') AS currency_contract,
        'Buy' AS category -- 'Purchase'
    FROM rarible."ExchangeV2_evt_Match"
    WHERE "leftAsset"->>'assetClass' = '0x8ae85d84'
    AND (
        "rightAsset"->>'assetClass' = '0x973bb640' 
        OR
        "rightAsset"->>'assetClass' = '0x73ad2146' 
        )
    AND evt_block_time >= start_ts
    AND evt_block_time < end_ts
),

royalty_fees AS 
    (
    SELECT tx_hash,
           traces.to as tx_to, 
           MIN(value) as fees
    FROM ethereum.traces
    WHERE block_time >= start_ts
    AND block_time < end_ts
    GROUP BY 1,2),
-- Get ERC721 and ERC1155 transfer data for every trade transaction
rarible_erc_union AS (
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
INNER JOIN rarible_trades d ON erc721.evt_tx_hash = d.evt_tx_hash
WHERE erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
AND erc721."from" <> '\x0000000000000000000000000000000000000000' -- Exclude mints
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
INNER JOIN rarible_trades d ON erc1155.evt_tx_hash = d.evt_tx_hash
WHERE erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
AND erc1155."from" <> '\x0000000000000000000000000000000000000000' -- Exclude mints
),
-- aggregate NFT transfers per transaction 
rarible_erc_subsets AS (
SELECT
    evt_tx_hash,
    fees/10 ^18 as fees,
    CASE WHEN erc_type = 'erc1155' THEN value
         WHEN erc_type = 'erc721'  THEN cardinality(array_agg(DISTINCT "tokenId")) END AS no_of_transfers,
    array_agg(DISTINCT "from") AS from_array,
    array_agg(DISTINCT "to") AS to_array,
    array_agg(DISTINCT erc_type) AS erc_type_array
FROM rarible_erc_union
LEFT JOIN royalty_fees rf ON rf.tx_hash = evt_tx_hash AND rf.tx_to = "from"
GROUP BY 1,erc_type,value,fees
),

rows AS (
    INSERT INTO nft.trades(
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
    DISTINCT trades.evt_block_time AS block_time,
    tokens.name AS nft_project_name,
    -- Set NFT token ID to `NULL` if the trade consists of multiple NFT transfers
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE nft_token_id END AS nft_token_id,
    -- Set ERC standard to `NULL` if the trade consists of multiple NFT transfers
    CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
    trades.platform,
    trades.platform_version,
    CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
    cast(erc.no_of_transfers as numeric) AS number_of_items,
    trades.category,
    trades.evt_type,
    agg.name AS aggregator,
    trades.price / 10 ^ erc20.decimals * p.price AS usd_amount,
        COALESCE(erc.from_array[1], trades.seller) AS seller,
        COALESCE(erc.to_array[1], trades.buyer) AS buyer,
    trades.price / 10 ^ erc20.decimals AS original_amount,
    trades.price AS original_amount_raw,
    CASE WHEN trades.original_currency_contract =  '\x0000000000000000000000000000000000000000'  THEN trades.price / 10 ^ erc20.decimals
         ELSE trades.price / 10 ^ erc20.decimals * p.price / peth.price END AS eth_amount,
    ROUND(cast((fees - 2.5*(trades.price / 10 ^ erc20.decimals)/100) * (100) / (NULLIF(trades.price / 10 ^ erc20.decimals,0))as numeric),7) as royalty_fees_percent,
    ROUND(cast(fees - 2.5*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7) as original_royalty_fees,
    ROUND(cast(fees * p.price - 2.5 * (trades.price / 10 ^ erc20.decimals * p.price) / 100 as numeric),7)   as usd_royalty_fees,
    2.5 as platform_fees_percent,
    ROUND(cast(2.5*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7) as original_platform_fees,
    ROUND(cast(2.5*(trades.price / 10 ^ erc20.decimals * p.price)/100 as numeric),7) as usd_platform_fees,
    CASE WHEN trades.original_currency_contract = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
    trades.original_currency_contract AS original_currency_contract,
    trades.currency_contract AS currency_contract,
    trades.nft_contract_address,
    trades.exchange_contract_address,
    trades.evt_tx_hash AS tx_hash,
    trades.evt_block_number,
    tx."from" AS tx_from,
    tx."to" AS tx_to,
    NULL::integer[] as trace_address,
    trades.evt_index,
    row_number() OVER (PARTITION BY platform, trades.evt_tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
        rarible_trades trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN rarible_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_contract
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN nft.aggregators agg ON agg.contract_address = tx."to"
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = trades.currency_contract
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN prices.usd peth ON peth.minute = date_trunc('minute', trades.evt_block_time)
        AND peth.symbol = 'WETH'
        AND peth.minute >= start_ts
        AND peth.minute < end_ts
    WHERE   trades.evt_block_time >= start_ts
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
    SELECT nft.insert_rarible(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='Rarible'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='Rarible')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/ 
