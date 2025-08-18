{% macro dex_wash_trades(blockchain) %}

WITH filtered_trades AS (
    SELECT *
    FROM {{ ref('dex_trades') }}
    WHERE blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', NOW() - interval '7' day)
    {% endif %}
),

-- Filter 1: True self-trading (same address on both sides)
filter_1_self_trading AS (
    SELECT tx_hash, true AS self_trading
    FROM filtered_trades
    WHERE tx_from = tx_to  -- Actual self-trading
),

-- Filter 2: Circular trading within single transaction (more restrictive)
filter_2_circular_same_tx AS (
    SELECT DISTINCT tx_hash, true AS circular_trading
    FROM (
        SELECT 
            ft1.tx_hash,
            tx_stats.unique_pairs,
            tx_stats.total_trades,
            -- Check for reciprocal pairs using self-join instead of EXISTS
            CASE WHEN ft2.tx_hash IS NOT NULL THEN 1 ELSE 0 END as has_reciprocal
        FROM filtered_trades ft1
        LEFT JOIN filtered_trades ft2 
            ON ft1.tx_hash = ft2.tx_hash 
            AND ft1.tx_from = ft2.tx_to 
            AND ft1.tx_to = ft2.tx_from
            AND ft1.token_bought_address = ft2.token_sold_address
            AND ft1.token_sold_address = ft2.token_bought_address
        INNER JOIN (
            SELECT 
                tx_hash,
                COUNT(DISTINCT CONCAT(CAST(tx_from AS varchar), '-', CAST(tx_to AS varchar))) as unique_pairs,
                COUNT(*) as total_trades
            FROM filtered_trades
            GROUP BY tx_hash
        ) tx_stats ON ft1.tx_hash = tx_stats.tx_hash
    ) subq
    WHERE has_reciprocal = 1     -- Must have reciprocal trades
    AND unique_pairs <= 3        -- Limited participants
    AND total_trades >= 4        -- Minimum complexity
),

-- Filter 3: Suspicious volume patterns (same wallet, high volume, minimal value extracted)
filter_3_suspicious_volume AS (
    SELECT tx_hash, true AS suspicious_volume
    FROM (
        SELECT 
            tx_hash,
            COUNT(*) as trade_count,
            SUM(amount_usd) as total_volume,
            -- Calculate if wallet is both major buyer and seller
            SUM(CASE WHEN tx_from = wallet_addr THEN amount_usd ELSE CAST(0.0 AS DECIMAL(38,18)) END) as sell_volume,
            SUM(CASE WHEN tx_to = wallet_addr THEN amount_usd ELSE CAST(0.0 AS DECIMAL(38,18)) END) as buy_volume
        FROM (
            -- Get all unique wallets involved in the transaction
            SELECT tx_hash, tx_from as wallet_addr, tx_from, tx_to, amount_usd FROM filtered_trades
            UNION ALL
            SELECT tx_hash, tx_to as wallet_addr, tx_from, tx_to, amount_usd FROM filtered_trades
        ) wallet_trades
        GROUP BY tx_hash, wallet_addr
    ) subq
    WHERE trade_count >= 6  -- Minimum trades for suspicion
    AND total_volume > 50000.0  -- Significant volume
    AND sell_volume > 0.0 AND buy_volume > 0.0  -- Wallet both buys and sells
    AND ABS(sell_volume - buy_volume) / NULLIF(GREATEST(sell_volume, buy_volume), 0.0) < 0.05  -- Nearly balanced
),

-- Filter 4: Related wallet patterns (simplified for Trino compatibility)
filter_4_related_wallets AS (
    SELECT tx_hash, true AS related_wallets
    FROM (
        SELECT 
            tx_hash,
            COUNT(DISTINCT tx_from) as unique_senders,
            COUNT(DISTINCT tx_to) as unique_receivers,
            COUNT(*) as total_trades
        FROM filtered_trades ft
        GROUP BY tx_hash
    ) subq
    WHERE total_trades >= 8
    AND unique_senders = unique_receivers  -- Same number of senders/receivers
    AND unique_senders <= 3  -- Limited participants
),

-- Filter 5: Token manipulation patterns
filter_5_token_manipulation AS (
    SELECT tx_hash, true AS token_manipulation
    FROM (
        SELECT 
            tx_hash,
            COUNT(DISTINCT token_bought_address) as unique_tokens,
            COUNT(*) as total_trades,
            -- Check for artificial volume creation on low-liquidity tokens
            AVG(amount_usd) as avg_trade_size,
            MAX(amount_usd) as max_trade_size,
            MIN(amount_usd) as min_trade_size
        FROM filtered_trades
        GROUP BY tx_hash
    ) subq
    WHERE unique_tokens <= 2  -- Focus on 1-2 tokens
    AND total_trades >= 8     -- High frequency
    AND max_trade_size / COALESCE(NULLIF(min_trade_size, 0.0), 1.0) > 100.0  -- Suspicious size variation
    AND avg_trade_size > 10000.0  -- Significant average size
)

SELECT 
    dt.*,
    -- Filter flags (more conservative)
    COALESCE(f1.self_trading, false) AS filter_1_self_trading,
    COALESCE(f2.circular_trading, false) AS filter_2_circular_same_tx,
    COALESCE(f3.suspicious_volume, false) AS filter_3_suspicious_volume,
    COALESCE(f4.related_wallets, false) AS filter_4_related_wallets,
    COALESCE(f5.token_manipulation, false) AS filter_5_token_manipulation,
    
    -- Combined wash trade flag (require multiple indicators for higher confidence)
    CASE WHEN 
        -- High confidence: Self-trading or circular trading
        COALESCE(f1.self_trading, false) OR COALESCE(f2.circular_trading, false)
        OR 
        -- Medium confidence: Multiple suspicious indicators
        ((CASE WHEN COALESCE(f3.suspicious_volume, false) THEN 1 ELSE 0 END) + 
         (CASE WHEN COALESCE(f4.related_wallets, false) THEN 1 ELSE 0 END) + 
         (CASE WHEN COALESCE(f5.token_manipulation, false) THEN 1 ELSE 0 END)) >= 2
    THEN true
    ELSE false
    END AS is_wash_trade,
    
    -- Confidence score, keeping only for testing, will remove for prod
    ((CASE WHEN COALESCE(f1.self_trading, false) THEN 3 ELSE 0 END) + 
     (CASE WHEN COALESCE(f2.circular_trading, false) THEN 3 ELSE 0 END) +
     (CASE WHEN COALESCE(f3.suspicious_volume, false) THEN 1 ELSE 0 END) + 
     (CASE WHEN COALESCE(f4.related_wallets, false) THEN 2 ELSE 0 END) + 
     (CASE WHEN COALESCE(f5.token_manipulation, false) THEN 2 ELSE 0 END)) AS wash_trade_confidence_score

FROM filtered_trades dt
LEFT JOIN filter_1_self_trading f1 ON dt.tx_hash = f1.tx_hash
LEFT JOIN filter_2_circular_same_tx f2 ON dt.tx_hash = f2.tx_hash
LEFT JOIN filter_3_suspicious_volume f3 ON dt.tx_hash = f3.tx_hash
LEFT JOIN filter_4_related_wallets f4 ON dt.tx_hash = f4.tx_hash
LEFT JOIN filter_5_token_manipulation f5 ON dt.tx_hash = f5.tx_hash

{% endmacro %}