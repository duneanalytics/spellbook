-- Test query for stablecoin_ethereum_transfers model
-- This query validates the model by checking key metrics and data quality

WITH daily_stats AS (
    SELECT
        block_date,
        COUNT(*) as transfer_count,
        COUNT(DISTINCT tx_hash) as unique_transactions,
        COUNT(DISTINCT contract_address) as unique_stablecoins,
        COUNT(DISTINCT "from") as unique_senders,
        COUNT(DISTINCT "to") as unique_receivers,
        SUM(amount_usd) as total_volume_usd,
        AVG(amount_usd) as avg_transfer_usd
    FROM stablecoin_ethereum.transfers
    WHERE block_date >= DATE '2024-10-01'
        AND block_date < DATE '2024-10-08'
    GROUP BY 1
),

stablecoin_summary AS (
    SELECT
        symbol,
        backing,
        denomination,
        stablecoin_name,
        COUNT(*) as transfer_count,
        SUM(amount_usd) as total_volume_usd,
        COUNT(DISTINCT "from") as unique_senders,
        COUNT(DISTINCT "to") as unique_receivers
    FROM stablecoin_ethereum.transfers
    WHERE block_date >= DATE '2024-10-01'
        AND block_date < DATE '2024-10-08'
    GROUP BY 1, 2, 3, 4
),

backing_distribution AS (
    SELECT
        backing,
        COUNT(*) as transfer_count,
        SUM(amount_usd) as volume_usd,
        COUNT(DISTINCT contract_address) as num_stablecoins
    FROM stablecoin_ethereum.transfers
    WHERE block_date >= DATE '2024-10-01'
        AND block_date < DATE '2024-10-08'
    GROUP BY 1
),

denomination_distribution AS (
    SELECT
        denomination,
        COUNT(*) as transfer_count,
        SUM(amount_usd) as volume_usd,
        COUNT(DISTINCT contract_address) as num_stablecoins
    FROM stablecoin_ethereum.transfers
    WHERE block_date >= DATE '2024-10-01'
        AND block_date < DATE '2024-10-08'
    GROUP BY 1
)

-- Main summary output
SELECT
    'Daily Stats' as metric_type,
    CAST(block_date AS VARCHAR) as dimension,
    transfer_count as count_value,
    total_volume_usd as volume_value
FROM daily_stats

UNION ALL

SELECT
    'Top Stablecoins' as metric_type,
    symbol || ' (' || backing || ')' as dimension,
    transfer_count as count_value,
    total_volume_usd as volume_value
FROM stablecoin_summary
ORDER BY volume_value DESC
LIMIT 10

UNION ALL

SELECT
    'By Backing Type' as metric_type,
    backing as dimension,
    transfer_count as count_value,
    volume_usd as volume_value
FROM backing_distribution

UNION ALL

SELECT
    'By Denomination' as metric_type,
    denomination as dimension,
    transfer_count as count_value,
    volume_usd as volume_value
FROM denomination_distribution

ORDER BY metric_type, volume_value DESC;

