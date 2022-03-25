CREATE OR REPLACE FUNCTION nft.insert_looksrare(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS INTEGER
LANGUAGE plpgsql AS $function$
DECLARE r INTEGER;
BEGIN

WITH looks_rare AS (
        SELECT 
        ask.evt_block_time AS block_time,
        ask."tokenId" AS token_id,
        ask.amount AS number_of_items,
        taker AS seller,
        maker AS buyer,
        price AS price,
        roy.amount AS fees,
        CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN ask.currency = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE ask.currency
        END AS currency_token,
        ask.currency AS original_currency_address,
        ask.collection AS nft_contract_address,
        ask.contract_address AS contract_address,
        ask.evt_tx_hash AS tx_hash,
        ask.evt_block_number AS block_number,
        ask.evt_index AS evt_index,
        CASE -- CATEGORIZE Collection Wide Offers Accepted 
            WHEN strategy = '\x86f909f70813cdb1bc733f4d97dc6b03b8e7e8f3' THEN 'Collection Offer Accepted'
            ELSE 'Offer Accepted' 
            END AS category    
    FROM looksrare."LooksRareExchange_evt_TakerAsk" ask
    LEFT JOIN looksrare."LooksRareExchange_evt_RoyaltyPayment" roy ON roy.evt_tx_hash = ask.evt_tx_hash
                            UNION ALL
    SELECT 
        bid.evt_block_time AS block_time,
        bid."tokenId" AS token_id,
        bid.amount AS number_of_items,
        maker AS seller,
        taker AS buyer,
        price AS price,
        roy.amount AS fees,
       CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN bid.currency = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE bid.currency
        END AS currency_token,
        bid.currency AS original_currency_address,
        bid.collection AS nft_contract_address,
        bid.contract_address AS contract_address,
        bid.evt_tx_hash AS tx_hash,
        bid.evt_block_number AS block_number,
        bid.evt_index AS evt_index,
        'Buy' as category
    FROM looksrare."LooksRareExchange_evt_TakerBid" bid
    LEFT JOIN looksrare."LooksRareExchange_evt_RoyaltyPayment" roy ON roy.evt_tx_hash = bid.evt_tx_hash
    ),

-- Get ERC721 AND ERC1155 transfer data for every trade TRANSACTION
erc_union AS (
SELECT DISTINCT
    erc721.evt_tx_hash,
    erc721.evt_index,
    'erc721' AS erc_type,
    CAST(erc721."tokenId" AS TEXT) AS "tokenId",
    erc721."from",
    erc721."to",
    erc721.contract_address,
    NULL::NUMERIC AS VALUE
FROM erc721."ERC721_evt_Transfer" erc721

INNER JOIN (
            Select evt_tx_hash 
            from looksrare."LooksRareExchange_evt_TakerAsk"
            UNION ALL
            Select evt_tx_hash 
            from looksrare."LooksRareExchange_evt_TakerBid"
            ) hashes on hashes.evt_tx_hash = erc721.evt_tx_hash
UNION ALL
SELECT DISTINCT
    erc1155.evt_tx_hash,
    erc1155.evt_index,
    'erc1155' AS erc_type,
    CAST(erc1155.id AS TEXT) AS "tokenId",
    erc1155."from",
    erc1155."to",
    erc1155.contract_address,
    erc1155.value
FROM erc1155."ERC1155_evt_TransferSingle" erc1155

INNER JOIN (
            Select evt_tx_hash 
            from looksrare."LooksRareExchange_evt_TakerAsk"
            UNION ALL
            Select evt_tx_hash 
            from looksrare."LooksRareExchange_evt_TakerBid"
            ) hashes on  erc1155.evt_tx_hash = hashes.evt_tx_hash
),

-- aggregate NFT transfers per TRANSACTION 
looksrare_erc_subsets AS (
SELECT 
    evt_tx_hash,
    CASE WHEN erc_type = 'erc1155' THEN value
         WHEN erc_type = 'erc721'  THEN cardinality(array_agg(DISTINCT "tokenId")) END AS no_of_transfers,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(evt_index) AS evt_index_array
FROM erc_union
GROUP BY 1,erc_type,value
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

   SELECT DISTINCT
        trades.block_time AS block_time,
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE tokens.name END AS nft_project_name,
        -- SET NFT token ID to `NULL` IF the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE token_id END AS nft_token_id,
        -- SET ERC standard to `NULL` IF the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
        trades.platform,
        trades.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        cast(erc.no_of_transfers as numeric) AS number_of_items,
        trades.category,
        trades.evt_type,
        agg.name AS aggregator,
        CASE WHEN erc.no_of_transfers > 1 THEN tx.value / 10 ^ erc20.decimals * p.price
        ELSE trades.price / 10 ^ erc20.decimals * p.price END AS usd_amount,
        CASE WHEN erc.no_of_transfers > 1 THEN NULL
        ELSE trades.seller END AS seller,
        trades.buyer,
        
        CASE WHEN erc.no_of_transfers > 1 THEN tx.value / 10 ^ erc20.decimals
        ELSE trades.price / 10 ^ erc20.decimals END AS original_amount,
        
        CASE WHEN erc.no_of_transfers > 1 THEN tx.value
        ELSE trades.price END AS original_amount_raw,

        CASE WHEN erc.no_of_transfers > 1 THEN tx.value / 10 ^ erc20.decimals * p.price / peth.price
            ELSE trades.price / 10 ^ erc20.decimals * p.price / peth.price END AS eth_amount,
        
        CASE WHEN erc.no_of_transfers > 1 THEN NULL::numeric
        ELSE ROUND(cast((trades.fees/ 10 ^ erc20.decimals - 2.5*(trades.price / 10 ^ erc20.decimals)/100) * (100) / NULLIF(trades.price / 10 ^ erc20.decimals,0) as numeric),7) END AS royalty_fees_percent,
        
        CASE WHEN erc.no_of_transfers > 1 THEN NULL::numeric
        ELSE ROUND(cast(trades.fees/ 10 ^ erc20.decimals - 2.5*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7) END AS original_royalty_fees,
        
        CASE WHEN erc.no_of_transfers > 1 THEN NULL::numeric
        ELSE ROUND(cast(trades.fees * p.price/ 10 ^ erc20.decimals - 2.5 * (trades.price / 10 ^ erc20.decimals * p.price) / 100 as numeric),7) END AS usd_royalty_fees,
        
        2 as platform_fees_percent,
        
        CASE WHEN erc.no_of_transfers > 1 THEN ROUND(cast(2*(tx.value / 10 ^ erc20.decimals)/100 as numeric),7) 
        ELSE ROUND(cast(2*(trades.price / 10 ^ erc20.decimals)/100 as numeric),7) END AS original_platform_fees,
        
        CASE WHEN erc.no_of_transfers > 1 THEN ROUND(cast(2*(tx.value / 10 ^ erc20.decimals * p.price)/100 as numeric),7)
        ELSE ROUND(cast(2*(trades.price / 10 ^ erc20.decimals * p.price)/100 as numeric),7) END AS usd_platform_fees,
        
        CASE WHEN trades.original_currency_address = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        trades.original_currency_address AS original_currency_contract,
        trades.currency_token AS currency_contract,
        COALESCE(erc.contract_address_array[1], trades.nft_contract_address) AS nft_contract_address,
        trades.contract_address AS exchange_contract_address,
        trades.tx_hash AS tx_hash,
        trades.block_number,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL::integer[] as trace_address,
        CASE WHEN erc.no_of_transfers > 1 THEN evt_index_array[1]
        ELSE trades.evt_index END AS evt_index,
        ROW_NUMBER() OVER (PARTITION BY trades.platform, trades.tx_hash, trades.evt_index, trades.category ORDER BY trades.platform_version, trades.evt_type) AS trade_id
    FROM
         (SELECT 
            'LooksRare' AS platform,
            '1' AS platform_version,
            category,
            'Trade' AS evt_type,
            price,
            fees,
            currency_token,
            contract_address,
            nft_contract_address,
            tx_hash,
            block_number,
            evt_index,
            block_time,
            token_id,
            seller,
            buyer,
            original_currency_address
        FROM looks_rare) trades
    INNER JOIN ethereum.transactions tx ON trades.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN looksrare_erc_subsets erc ON erc.evt_tx_hash = trades.tx_hash
    LEFT JOIN nft.tokens tokens ON tokens.contract_address =  trades.nft_contract_address
    LEFT JOIN nft.aggregators agg ON agg.contract_address = tx."to"
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.contract_address = trades.currency_token
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN prices.usd peth ON peth.minute = date_trunc('minute', trades.block_time)
        AND peth.symbol = 'WETH'
        AND peth.minute >= start_ts
        AND peth.minute < end_ts
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_token
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT COUNT(*) INTO r FROM ROWS;
RETURN r;
END
$function$;

/*
INSERT INTO cron.job (schedule, command)
VALUES ('47 * * * *', $$
    SELECT nft.insert_looksrare(
        (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='LooksRare'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='LooksRare')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
*/
