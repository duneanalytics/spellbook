-- table start fill
-- v3
SELECT dex.insert_oneinch(
    '2021-11-11',
    now(),
	3
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = '1inch' AND version = '3'
);
--v4
SELECT dex.insert_oneinch(
    '2021-11-11',
    now(),
	4
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = '1inch' AND version = '4'
);


-- fill 2021 (post-regenesis 11-11)
SELECT dex.insert_uniswap_v3(
    '2021-11-10',
    now()
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-11-10'
    AND block_time <= now() - interval '20 minutes'
    AND project = 'Uniswap' AND version = '3'
);


-- fill 2021, 0x only launched after op2
SELECT dex.insert_zeroex(
    '2021-12-28',
    now(),
    0,
    (SELECT MAX(number) FROM optimism.blocks where time < now() - interval '20 minutes')
)
WHERE NOT EXISTS (
    SELECT *
    FROM dex.trades
    WHERE block_time > '2021-12-28'
    AND block_time <= now() - interval '20 minutes'
    AND project IN ('0x API', 'Matcha')
);
