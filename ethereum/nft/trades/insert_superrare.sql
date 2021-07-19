CREATE OR REPLACE FUNCTION nft.insert_superrare(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH all_trades AS (
    SELECT
	'SuperRare' AS platform,
        tx_hash,
        block_time,
        CAST(bytea2numericpy(substring(data FROM 33)) as TEXT) AS token_id,
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
erc721_distinct AS (
SELECT
    DISTINCT erc721.contract_address AS contract_address,
    evt_tx_hash,
    first_value("from") OVER (PARTITION BY evt_tx_hash ORDER BY erc721.evt_index) as "from",
    first_value("to") OVER  (PARTITION BY evt_tx_hash ORDER BY erc721.evt_index DESC) as "to"
FROM erc721."ERC721_evt_Transfer" erc721 
INNER JOIN all_trades ON all_trades.tx_hash = erc721.evt_tx_hash
),
erc20_distinct AS (
SELECT
    DISTINCT erc20.contract_address AS contract_address,
    evt_tx_hash,
    first_value("from") OVER (PARTITION BY evt_tx_hash ORDER BY erc20.evt_index) as "from",
    first_value("to") OVER (PARTITION BY evt_tx_hash ORDER BY erc20.evt_index DESC) as "to"
FROM erc20."ERC20_evt_Transfer" erc20 
INNER JOIN all_trades ON all_trades.tx_hash = erc20.evt_tx_hash
WHERE erc20.contract_address <> '\x0f5d2fb29fb7d3cfee444a200298f468908cc942'
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
	trades.block_time,
        tokens.name AS nft_project_name,
	trades.token_id AS nft_token_id,
	trades.platform,
	trades.platform_version,
	category,
	evt_type,
	trades.original_amount_raw / 10^18 * p.price AS usd_amount,
	COALESCE(erc721."from", erc20."from") AS seller,
	COALESCE(erc721."to", erc20."to") AS buyer,
	trades.original_amount_raw / 10^18 as original_amount,
	trades.original_amount_raw as original_amount_raw,
	'ETH' AS original_currency,
	'\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract, 
	'\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
	COALESCE(erc721.contract_address, erc20.contract_address) AS nft_contract_address,
	trades.exchange_contract_address,
	trades.tx_hash,
	trades.block_number,
	tx."from" AS tx_from,
	tx."to" AS tx_to,
	NULL::integer[] AS trace_address,
	trades.evt_index,
        row_number() OVER (PARTITION BY platform, trades.tx_hash, trades.evt_index, category ORDER BY platform_version, evt_type) AS trade_id
    FROM
	all_trades trades
    INNER JOIN ethereum.transactions tx
        ON trades.tx_hash = tx.hash
        AND tx.block_time >= start_ts
        AND tx.block_time < end_ts
        AND tx.block_number >= start_block
        AND tx.block_number < end_block
    LEFT JOIN prices.usd p ON p.minute = date_trunc('minute', trades.block_time)
        AND p.contract_address = '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
        AND p.minute >= start_ts
        AND p.minute < end_ts
    LEFT JOIN erc721_distinct erc721 ON trades.tx_hash = erc721.evt_tx_hash
    LEFT JOIN erc20_distinct erc20 ON trades.tx_hash = erc20.evt_tx_hash
    LEFT JOIN nft.tokens tokens ON tokens.contract_address = COALESCE(erc721.contract_address, erc20.contract_address)
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
