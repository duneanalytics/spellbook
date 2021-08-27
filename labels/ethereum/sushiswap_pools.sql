-- https://duneanalytics.com/queries/87493
-- Sushiswap v1
WITH sushi_v1_first_token AS (
SELECT
    pair AS pool_address,
    t.symbol as token_0_symbol
FROM sushi."Factory_evt_PairCreated" dex
INNER JOIN erc20.tokens t ON dex."token0" = t.contract_address
WHERE t.symbol IS NOT NULL
),
sushi_v1_second_token AS (
SELECT
    pair AS pool_address,
    t.symbol as token_1_symbol
FROM sushi."Factory_evt_PairCreated" dex
INNER JOIN erc20.tokens t ON dex."token1" = t.contract_address
WHERE t.symbol IS NOT NULL
)

-- Main Query
SELECT
    f.pool_address AS address,
    LOWER(CONCAT('Sushi v1 LP ', token_0_symbol, ' - ', token_1_symbol)) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM sushi_v1_first_token f
INNER JOIN sushi_v1_second_token s ON f.pool_address = s.pool_address
