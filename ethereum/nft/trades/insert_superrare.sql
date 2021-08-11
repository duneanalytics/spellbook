CREATE OR REPLACE FUNCTION nft.insert_superrare(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH superrare_trades AS (
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
-- Get ERC721 and ERC1155 transfer data for every trade transaction
-- as well as ERC20 and custom `transfer` data for specific SuperRare contracts
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
AND erc721."from" <> '\x0000000000000000000000000000000000000000' -- exclude mints
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
AND erc1155."from" <> '\x0000000000000000000000000000000000000000' -- exclude mints
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
AND erc20."from" <> '\x0000000000000000000000000000000000000000' -- exclude mints
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
AND st."_from" <> '\x0000000000000000000000000000000000000000' -- exclude mints
AND st.contract_address = '\x41a322b28d0ff354040e2cbc676f0320d8c8850d'
),
-- aggregate NFT transfers per transaction 
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
	trades.block_time,
        tokens.name AS nft_project_name,
        -- Set NFT token ID to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE trades.token_id END AS nft_token_id,
        -- Set ERC standard to `NULL` if the trade consists of multiple NFT transfers
        CASE WHEN erc.no_of_transfers > 1 THEN NULL ELSE COALESCE(erc.erc_type_array[1], tokens.standard) END AS erc_standard,
	trades.platform,
	trades.platform_version,
        CASE WHEN erc.no_of_transfers > 1 THEN 'Bundle Trade' ELSE 'Single Item Trade' END AS trade_type,
        erc.no_of_transfers AS number_of_items,
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
        -- Sometimes multiple NFT transfers occur in a given trade; the 'array' fields below provide info for these use cases 
        erc.token_id_array AS nft_token_ids_array,
        erc.from_array AS senders_array,
        erc.to_array AS recipients_array,
        erc.erc_type_array AS erc_types_array,
        erc.contract_address_array AS nft_contract_addresses_array,
        erc.erc1155_value_array AS erc_values_array,
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
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN superrare_erc_subsets erc ON erc.evt_tx_hash = trades.tx_hash
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = erc.contract_address_array[1]
    WHERE category IN ('Buy','Offer Accepted','Auction Settled')
    AND trades.block_time >= start_ts
    AND trades.block_time < end_ts
    ON CONFLICT DO NOTHING
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;

-- fill 2019
SELECT nft.insert_superrare(
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
    AND platform = 'SuperRare'
);


-- fill 2020
SELECT nft.insert_superrare(
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
    AND platform = 'SuperRare'
);

-- fill 2021
SELECT nft.insert_superrare(
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
    AND platform = 'SuperRare'
);

INSERT INTO cron.job (schedule, command)
VALUES ('53 * * * *', $$
    SELECT nft.insert_superrare(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='SuperRare'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='SuperRare')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
