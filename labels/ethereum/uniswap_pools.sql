-- Uniswap v3
WITH uni_v3_first_token AS (
SELECT
    pool AS pool_address,
    t.symbol as token_0_symbol
FROM uniswap_v3."Factory_evt_PoolCreated" dex
INNER JOIN erc20.tokens t ON dex."token0" = t.contract_address
WHERE t.symbol IS NOT NULL
),
uni_v3_second_token AS (
SELECT
    pool AS pool_address,
    t.symbol as token_1_symbol
FROM uniswap_v3."Factory_evt_PoolCreated" dex
INNER JOIN erc20.tokens t ON dex."token1" = t.contract_address
WHERE t.symbol IS NOT NULL
),

-- Uniswap v2
uni_v2_first_token AS (
SELECT
    pair AS pool_address,
    t.symbol as token_0_symbol
FROM uniswap_v2."Factory_evt_PairCreated" dex
INNER JOIN erc20.tokens t ON dex."token0" = t.contract_address
WHERE t.symbol IS NOT NULL
),
uni_v2_second_token AS (
SELECT
    pair AS pool_address,
    t.symbol as token_1_symbol
FROM uniswap_v2."Factory_evt_PairCreated" dex
INNER JOIN erc20.tokens t ON dex."token1" = t.contract_address
WHERE t.symbol IS NOT NULL
),

-- Uniswap v1
uni_v1_token AS (
SELECT
    exchange AS pool_address,
    t.symbol as token_0_symbol
FROM uniswap."Factory_evt_NewExchange" dex
INNER JOIN erc20.tokens t ON dex.token = t.contract_address
WHERE t.symbol IS NOT NULL
)

-- Main Query
SELECT
    pool_address AS address,
    LOWER(CONCAT('Uni v1 LP ', token_0_symbol, ' - ', 'ETH')) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM uni_v1_token
UNION ALL
SELECT
    f.pool_address AS address,
    LOWER(CONCAT('Uni v2 LP ', token_0_symbol, ' - ', token_1_symbol)) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM uni_v2_first_token f
INNER JOIN uni_v2_second_token s ON f.pool_address = s.pool_address
UNION ALL
SELECT
    f.pool_address AS address,
    LOWER(CONCAT('Uni v3 LP ', token_0_symbol, ' - ', token_1_symbol)) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM uni_v3_first_token f
INNER JOIN uni_v3_second_token s ON f.pool_address = s.pool_address
