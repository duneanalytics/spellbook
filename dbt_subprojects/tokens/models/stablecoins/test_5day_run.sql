-- Test query for stablecoin_ethereum_transfers - Last 5 days
-- This simulates what the model will produce

WITH stablecoin_list AS (
    SELECT DISTINCT
        s.contract_address,
        s.symbol,
        s.backing,
        s.decimals,
        s.name,
        s.denomination
    FROM tokens_ethereum.erc20_stablecoins s
),

stablecoin_transfers AS (
    SELECT
        t.unique_key,
        t.blockchain,
        t.block_month,
        t.block_date,
        t.block_time,
        t.block_number,
        t.tx_hash,
        t.evt_index,
        t.trace_address,
        t.token_standard,
        t.tx_from,
        t.tx_to,
        t.tx_index,
        t."from",
        t."to",
        t.contract_address,
        t.symbol,
        t.amount_raw,
        t.amount,
        t.price_usd,
        t.amount_usd,
        s.backing,
        s.name AS stablecoin_name,
        s.denomination
    FROM tokens_ethereum.transfers t
    INNER JOIN stablecoin_list s
        ON t.contract_address = s.contract_address
    WHERE t.block_date >= CURRENT_DATE - INTERVAL '5' DAY
),

summary_stats AS (
    SELECT
        COUNT(*) as total_transfers,
        COUNT(DISTINCT tx_hash) as unique_transactions,
        COUNT(DISTINCT contract_address) as unique_stablecoins,
        COUNT(DISTINCT "from") as unique_senders,
        COUNT(DISTINCT "to") as unique_receivers,
        SUM(amount_usd) as total_volume_usd,
        AVG(amount_usd) as avg_transfer_usd,
        MIN(block_date) as earliest_date,
        MAX(block_date) as latest_date
    FROM stablecoin_transfers
),

daily_breakdown AS (
    SELECT
        block_date,
        COUNT(*) as transfers,
        COUNT(DISTINCT tx_hash) as transactions,
        COUNT(DISTINCT contract_address) as stablecoins_active,
        SUM(amount_usd) as volume_usd
    FROM stablecoin_transfers
    GROUP BY 1
    ORDER BY 1 DESC
),

top_stablecoins AS (
    SELECT
        symbol,
        backing,
        denomination,
        COUNT(*) as transfer_count,
        SUM(amount_usd) as total_volume_usd,
        COUNT(DISTINCT "from") as unique_senders,
        COUNT(DISTINCT "to") as unique_receivers
    FROM stablecoin_transfers
    GROUP BY 1, 2, 3
    ORDER BY total_volume_usd DESC
    LIMIT 15
),

backing_summary AS (
    SELECT
        backing,
        COUNT(*) as transfer_count,
        SUM(amount_usd) as volume_usd,
        COUNT(DISTINCT contract_address) as num_stablecoins
    FROM stablecoin_transfers
    GROUP BY 1
    ORDER BY volume_usd DESC
)

-- Output all summaries
SELECT 'OVERALL SUMMARY' as section, 
    CAST(total_transfers AS VARCHAR) as metric, 
    'Total Transfers' as description,
    CAST(total_volume_usd AS VARCHAR) as value_usd
FROM summary_stats

UNION ALL

SELECT 'OVERALL SUMMARY', 
    CAST(unique_transactions AS VARCHAR), 
    'Unique Transactions',
    ''
FROM summary_stats

UNION ALL

SELECT 'OVERALL SUMMARY', 
    CAST(unique_stablecoins AS VARCHAR), 
    'Unique Stablecoins',
    ''
FROM summary_stats

UNION ALL

SELECT 'OVERALL SUMMARY', 
    CAST(unique_senders AS VARCHAR), 
    'Unique Senders',
    ''
FROM summary_stats

UNION ALL

SELECT 'OVERALL SUMMARY', 
    CAST(unique_receivers AS VARCHAR), 
    'Unique Receivers',
    ''
FROM summary_stats

UNION ALL

SELECT 'OVERALL SUMMARY', 
    CAST(earliest_date AS VARCHAR), 
    'Date Range Start',
    ''
FROM summary_stats

UNION ALL

SELECT 'OVERALL SUMMARY', 
    CAST(latest_date AS VARCHAR), 
    'Date Range End',
    ''
FROM summary_stats

UNION ALL

SELECT 'DAILY BREAKDOWN', 
    CAST(block_date AS VARCHAR), 
    CAST(transfers AS VARCHAR) || ' transfers, ' || CAST(transactions AS VARCHAR) || ' txs',
    CAST(ROUND(volume_usd, 2) AS VARCHAR)
FROM daily_breakdown

UNION ALL

SELECT 'TOP STABLECOINS', 
    symbol || ' (' || backing || ')', 
    CAST(transfer_count AS VARCHAR) || ' transfers',
    CAST(ROUND(total_volume_usd, 2) AS VARCHAR)
FROM top_stablecoins

UNION ALL

SELECT 'BY BACKING TYPE', 
    backing, 
    CAST(transfer_count AS VARCHAR) || ' transfers, ' || CAST(num_stablecoins AS VARCHAR) || ' coins',
    CAST(ROUND(volume_usd, 2) AS VARCHAR)
FROM backing_summary

ORDER BY section, description DESC, CAST(value_usd AS DOUBLE) DESC;

