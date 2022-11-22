CREATE OR REPLACE FUNCTION nft.insert_opensea(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH wyvern_calldata AS (
    SELECT
        call_tx_hash,
        addrs [5] AS nft_contract_address,
        addrs [2] AS buyer,
        addrs [9] AS seller,
        addrs [7] AS original_currency_address,
        CASE -- Replace `ETH` with `WETH` for ERC20 lookup later
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
-- Get ERC721 and ERC1155 transfer data for every trade transaction
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
INNER JOIN opensea."WyvernExchange_evt_OrdersMatched" trades ON erc721.evt_tx_hash = trades.evt_tx_hash
WHERE erc721.evt_block_time >= start_ts
AND erc721.evt_block_time < end_ts
AND erc721."from" <> '\x0000000000000000000000000000000000000000' -- Exclude mints
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
INNER JOIN opensea."WyvernExchange_evt_OrdersMatched" trades ON erc1155.evt_tx_hash = trades.evt_tx_hash
WHERE erc1155.evt_block_time >= start_ts
AND erc1155.evt_block_time < end_ts
AND erc1155."from" <> '\x0000000000000000000000000000000000000000' -- Exclude mints
),
-- aggregate NFT transfers per transaction 
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
        tokens.name AS nft_project_name,
        -- Set NFT token ID to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE token_id END AS nft_token_id,
        -- Set ERC standard to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
        trades.platform,
        trades.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        erc.no_of_transfers AS number_of_items,
        trades.category,
        trades.evt_type,
        trades.price / 10 ^ erc20.decimals * p.price AS usd_amount,
        wc.seller,
        wc.buyer,
        trades.price / 10 ^ erc20.decimals AS original_amount,
        trades.price AS original_amount_raw,
        CASE WHEN wc.original_currency_address = '\x0000000000000000000000000000000000000000' THEN 'ETH' ELSE erc20.symbol END AS original_currency,
        wc.original_currency_address AS original_currency_contract,
        wc.currency_token AS currency_contract,
        COALESCE(erc.contract_address_array[1], wc.nft_contract_address) AS nft_contract_address,
        trades.contract_address AS exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number,
        -- Sometimes multiple NFT transfers occur in a given trade; the 'array' fields below provide info for these use cases 
        erc.token_id_array AS nft_token_ids_array,
        erc.from_array AS senders_array,
        erc.to_array AS recipients_array,
        erc.erc_type_array AS erc_types_array,
        erc.contract_address_array AS nft_contract_addresses_array,
        erc.erc1155_value_array AS erc_values_array,
        tx."from" AS tx_from,
        tx."to" AS tx_to,
        call_trace_address AS trace_address,
        trades.evt_index,
        row_number() OVER (PARTITION BY trades.platform, trades.evt_tx_hash, trades.evt_index, trades.category ORDER BY trades.platform_version, trades.evt_type) AS trade_id
    FROM
        (SELECT 
            'OpenSea' AS platform,
            '1' AS platform_version,
            'Buy' AS category,
            'Trade' AS evt_type,
            evt_block_time,
            price,
            contract_address,
            evt_tx_hash,
            evt_block_number,
            evt_index
        FROM opensea."WyvernExchange_evt_OrdersMatched") trades
    INNER JOIN ethereum.transactions tx
        ON trades.evt_tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN opensea_erc_subsets erc ON erc.evt_tx_hash = trades.evt_tx_hash
    LEFT JOIN wyvern_calldata wc ON wc.call_tx_hash = trades.evt_tx_hash
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = wc.nft_contract_address
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.evt_block_time)
        AND p.contract_address = wc.currency_token
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN erc20.tokens erc20 ON erc20.contract_address = wc.currency_token
    WHERE
        NOT EXISTS (SELECT * -- Exclude OpenSea mint transactions
            FROM erc721."ERC721_evt_Transfer" erc721
            WHERE trades.evt_tx_hash = erc721.evt_tx_hash
            AND erc721."from" = '\x0000000000000000000000000000000000000000')
        AND trades.evt_block_time >= start_ts
        AND trades.evt_block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2018
SELECT nft.insert_opensea(
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
    AND platform = 'OpenSea'
);

-- fill 2019
SELECT nft.insert_opensea(
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
    AND platform = 'OpenSea'
);


-- fill 2020
SELECT nft.insert_opensea(
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
    AND platform = 'OpenSea'
);

-- fill 2021
SELECT nft.insert_opensea(
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
    AND platform = 'OpenSea'
);

INSERT INTO cron.job (schedule, command)
VALUES ('47 * * * *', $$
    SELECT nft.insert_opensea(
        (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '6 hours' FROM nft.trades WHERE platform='OpenSea')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
