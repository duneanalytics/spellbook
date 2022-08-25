-- `StandardPoolConverter` contracts contain the token balances
-- e.g. https://etherscan.io/address/0x8df51a9714ae6357a5b829cc8d677b43d7e8bd53
WITH bancor_pools AS (
SELECT
    DISTINCT contract_address AS pool_address,
    CASE 
        WHEN "_reserveToken" = '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2'
        ELSE "_reserveToken"
    END AS token 
FROM bancor."StandardPoolConverter_evt_LiquidityAdded" dex
WHERE "_reserveToken" <> '\x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c'
)

-- Main Query
SELECT
    pool_address AS address,
    LOWER(CONCAT('Bancor LP ', t.symbol, ' - ', 'BNT')) AS label,
    'lp_pool_name' AS type,
    'masquot' AS author
FROM bancor_pools
INNER JOIN erc20.tokens t ON bancor_pools.token = t.contract_address
WHERE t.symbol IS NOT NULL
