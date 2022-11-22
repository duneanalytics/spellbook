CREATE OR REPLACE FUNCTION nft.insert_looksrare(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS INTEGER
LANGUAGE plpgsql AS $function$
DECLARE r INTEGER;
BEGIN

WITH looks_rare AS (
    SELECT 
        evt_block_time AS block_time,
        "tokenId" AS token_id,
        amount AS number_of_items,
        taker AS seller,
        maker AS buyer,
        price AS price,
        CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN currency = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE currency
        END AS currency_token,
        currency AS original_currency_address,
        collection AS nft_contract_address,
        contract_address AS contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index AS evt_index,
        'Buy' as category
    FROM looksrare."LooksRareExchange_evt_TakerAsk"
UNION ALL
    SELECT 
        evt_block_time AS block_time,
        "tokenId" AS token_id,
        amount AS number_of_items,
        maker AS seller,
        taker AS buyer,
        price AS price,
       CASE -- REPLACE `ETH` WITH `WETH` for ERC20 lookup later
            WHEN currency = '\x0000000000000000000000000000000000000000' THEN '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE currency
        END AS currency_token,
        currency AS original_currency_address,
        collection AS nft_contract_address,
        contract_address AS contract_address,
        evt_tx_hash AS tx_hash,
        evt_block_number AS block_number,
        evt_index AS evt_index,
        'Offer Accepted' as category
    FROM looksrare."LooksRareExchange_evt_TakerBid"
),


-- Get ERC721 AND ERC1155 transfer data for every trade TRANSACTION
erc_union AS (
SELECT
    erc721.evt_tx_hash,
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

SELECT
    erc1155.evt_tx_hash,
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
    array_agg("tokenId") AS token_id_array,
    cardinality(array_agg("tokenId")) AS no_of_transfers,
    array_agg("from") AS from_array,
    array_agg("to") AS to_array,
    array_agg(erc_type) AS erc_type_array,
    array_agg(contract_address) AS contract_address_array,
    array_agg(VALUE) AS erc1155_value_array
FROM erc_union
GROUP BY 1
),

ROWS AS (
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
        trades.block_time AS block_time,
        tokens.name AS nft_project_name,
        -- SET NFT token ID to `NULL` IF the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE token_id END AS nft_token_id,
        -- SET ERC standard to `NULL` IF the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
        trades.platform,
        trades.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        erc.no_of_transfers AS number_of_items,
        trades.category,
        trades.evt_type,
        trades.price / 10 ^ erc20.decimals * p.price AS usd_amount,
        trades.seller,
        trades.buyer,
        trades.price / 10 ^ erc20.decimals AS original_amount,
        trades.price AS original_amount_raw,
        CASE WHEN trades.original_currency_address = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        trades.original_currency_address AS original_currency_contract,
        trades.currency_token AS currency_contract,
        COALESCE(erc.contract_address_array[1], trades.nft_contract_address) AS nft_contract_address,
        trades.contract_address AS exchange_contract_address,
        trades.tx_hash AS tx_hash,
        trades.block_number,
        -- Sometimes multiple NFT transfers occur IN a given trade; the 'array' fields below provide info for these use cases 
        erc.token_id_array AS nft_token_ids_array,
        erc.from_array AS senders_array,
        erc.to_array AS recipients_array,
        erc.erc_type_array AS erc_types_array,
        erc.contract_address_array AS nft_contract_addresses_array,
        erc.erc1155_value_array AS erc_values_array,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        NULL AS trace_address,
        trades.evt_index,
        ROW_NUMBER() OVER (PARTITION BY trades.platform, trades.tx_hash, trades.evt_index, trades.category ORDER BY trades.platform_version, trades.evt_type) AS trade_id
    FROM
        (SELECT 
            'LooksRare' AS platform,
            '1' AS platform_version,
            category,
            'Trade' AS evt_type,
            price,
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
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = trades.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.minute >= start_ts
        AND p.minute < end_ts
        AND p.contract_address = trades.currency_token
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = trades.currency_token

    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT COUNT(*) INTO r FROM ROWS;
RETURN r;
END
$function$;

-- fill 2022
SELECT nft.insert_looksrare(
    '2021-01-01',
    now(),
    (SELECT max(number) FROM ethereum.blocks WHERE time < '2021-01-01'),
    (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM nft.trades
    WHERE block_time > '2022-01-01'
    AND block_time <= now() - interval '20 minutes'
    AND platform = 'LooksRare'
);


INSERT INTO cron.job (schedule, command)
VALUES ('47 * * * *', $$
    SELECT nft.insert_looksrare(
        (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='LooksRare'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='LooksRare')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
