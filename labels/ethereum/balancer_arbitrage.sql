SELECT
    DISTINCT(t.to) AS address,
    'arbitrage bot' AS label,
    'dapp usage' AS type,
    'balancerlabs' AS author
FROM dex.trades2 t1
INNER JOIN dex.trades2 t2
ON t1.tx_hash = t2.tx_hash
AND t1.token_a_address = t2.token_b_address
AND t1.token_b_address = t2.token_a_address
AND ((t1.project = 'Balancer' AND t2.project = 'Uniswap') or (t1.project = 'Uniswap' AND t2.project = 'Balancer'))
INNER JOIN ethereum.transactions t ON t.hash = t1.tx_hash
WHERE t1.block_time >= '{{timestamp}}'
AND t2.block_time >= '{{timestamp}}'
UNION ALL
SELECT
    DISTINCT(t.to) AS address,
    'arbitrage bot' AS label,
    'dapp usage' AS type,
    'balancerlabs' AS author
FROM dex.trades2 t1
INNER JOIN dex.trades2 t2
ON t1.tx_hash = t2.tx_hash
AND t1.token_a_address = t2.token_b_address
AND t1.token_b_address = t2.token_a_address
AND ((t1.project = 'Balancer' AND t2.project = 'Sushiswap') or (t1.project = 'Sushiswap' AND t2.project = 'Balancer'))
INNER JOIN ethereum.transactions t ON t.hash = t1.tx_hash
WHERE t1.block_time >= '{{timestamp}}'
AND t2.block_time >= '{{timestamp}}'
