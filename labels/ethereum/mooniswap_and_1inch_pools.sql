-- https://duneanalytics.com/queries/87581
-- 1inch
WITH pools_1inch AS (
    SELECT
        token1,
        token2,
        mooniswap
    FROM onelp."MooniswapFactory_evt_Deployed"
    UNION ALL
    SELECT
        token1,
        token2,
        mooniswap
    FROM onelp."MooniswapFactory_v2_evt_Deployed"
),
pools_1inch_first_token AS (
SELECT
    mooniswap AS pool_address,
    t.symbol as token_0_symbol
FROM onelp."MooniswapFactory_evt_Deployed" dex
INNER JOIN erc20.tokens t ON dex.token1 = t.contract_address
),
pools_1inch_second_token AS (
SELECT
    mooniswap AS pool_address,
    t.symbol as token_1_symbol
FROM onelp."MooniswapFactory_evt_Deployed" dex
INNER JOIN erc20.tokens t ON dex.token2 = t.contract_address
),

-- Mooniswap
mooniswap_first_token AS (
SELECT
    mooniswap AS pool_address,
    t.symbol as token_0_symbol
FROM mooniswap."MooniFactory_evt_Deployed" dex
INNER JOIN erc20.tokens t ON dex.token1 = t.contract_address
),
mooniswap_second_token AS (
SELECT
    mooniswap AS pool_address,
    t.symbol as token_1_symbol
FROM mooniswap."MooniFactory_evt_Deployed" dex
INNER JOIN erc20.tokens t ON dex.token2 = t.contract_address
)

-- Main Query
SELECT
    f.pool_address AS address,
    LOWER(CONCAT('1inch v1 LP ', token_0_symbol, ' - ', token_1_symbol)) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM pools_1inch_first_token f
INNER JOIN pools_1inch_second_token s ON f.pool_address = s.pool_address
UNION ALL
SELECT
    f.pool_address AS address,
    LOWER(CONCAT('Mooniswap v1 LP ', token_0_symbol, ' - ', token_1_symbol)) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM mooniswap_first_token f
INNER JOIN mooniswap_second_token s ON f.pool_address = s.pool_address
