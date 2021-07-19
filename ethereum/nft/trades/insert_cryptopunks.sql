CREATE OR REPLACE FUNCTION nft.insert_cryptopunks(start_ts timestamptz, end_ts timestamptz=now(), start_block numeric=0, end_block numeric=9e18) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN

WITH rows AS (
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
        'CryptoPunks' AS nft_project_name,
        CAST(trades."punkIndex" AS TEXT) AS nft_token_id,
        platform,
        platform_version,
        category,
        evt_type,
        trades.value / 10 ^ 18 * p.price AS usd_amount,
        trades."fromAddress" AS seller,
        trades."toAddress" AS buyer,
        trades.value / 10 ^ 18 AS original_amount,
        trades.value AS original_amount_raw,
        'ETH' AS original_currency,
        '\x0000000000000000000000000000000000000000'::bytea AS original_currency_contract,
        '\xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'::bytea AS currency_contract,
        trades.contract_address AS nft_contract_address,
        trades.contract_address AS exchange_contract_address,
        trades.evt_tx_hash AS tx_hash,
        trades.evt_block_number AS block_number,
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
            FROM cryptopunks."CryptoPunksMarket_evt_PunkBought") trades
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
    AND platform = 'CryptoPunks'
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
    AND platform = 'CryptoPunks'
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
    AND platform = 'CryptoPunks'
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
    AND platform = 'CryptoPunks'
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
    AND platform = 'CryptoPunks'
);

INSERT INTO cron.job (schedule, command)
VALUES ('53 * * * *', $$
    SELECT nft.insert_cryptopunks(
        (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='CryptoPunks'),
        (SELECT now() - interval '20 minutes'),
        (SELECT max(number) FROM ethereum.blocks WHERE time < (SELECT max(block_time) - interval '1 days' FROM nft.trades WHERE platform='CryptoPunks')),
        (SELECT MAX(number) FROM ethereum.blocks where time < now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
