{% macro dex_wash_trades(blockchain) %}

WITH base_filtered_trades AS (
    SELECT *
    FROM {{ ref('dex_trades') }}
    WHERE blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% else %}
    AND block_time >= date_trunc('day', NOW() - interval '3' day)
    {% endif %}
    AND amount_usd > 100  -- Filter small trades early
),

multi_trade_transactions AS (
    SELECT tx_hash
    FROM base_filtered_trades
    GROUP BY tx_hash
    HAVING COUNT(*) >= 2  -- Only multi-trade transactions
),

filtered_trades AS (
    SELECT bft.*
    FROM base_filtered_trades bft
    INNER JOIN multi_trade_transactions mtt ON bft.tx_hash = mtt.tx_hash
),

-- OPTIMIZATION: thin projection for heavy operations
thin_trades AS (
    SELECT 
        tx_hash,
        tx_from,
        tx_to,
        token_bought_address,
        token_sold_address,
        amount_usd
    FROM filtered_trades
),

-- OPTIMIZATION: precompute per-transaction stats once
tx_stats AS (
    SELECT 
        tx_hash,
        COUNT(DISTINCT CONCAT(CAST(tx_from AS varchar), '-', CAST(tx_to AS varchar))) as unique_pairs,
        COUNT(*) as total_trades
    FROM thin_trades
    GROUP BY tx_hash
),

-- NEW: Basic candidate filter to gate expensive operations
tx_basic_candidates AS (
    SELECT tx_hash
    FROM tx_stats
    WHERE total_trades >= 6  -- Only check complex transactions
),

-- Filter 1: True self-trading (same address on both sides)
filter_1_self_trading AS (
    SELECT tx_hash, true AS self_trading
    FROM thin_trades 
    WHERE tx_from = tx_to  -- Actual self-trading
),

-- Filter 2: Circular trading within single transaction (more restrictive) rewritten with EXISTS
filter_2_circular_same_tx AS (
    SELECT DISTINCT ts.tx_hash, true AS circular_trading
    FROM tx_stats ts
    WHERE ts.unique_pairs <= 3        -- Limited participants
      AND ts.total_trades >= 4        -- Minimum complexity
      AND EXISTS (
          SELECT 1
          FROM thin_trades ft1
          WHERE ft1.tx_hash = ts.tx_hash
            AND EXISTS (
                SELECT 1
                FROM thin_trades ft2
                WHERE ft2.tx_hash = ft1.tx_hash
                  AND ft1.tx_from = ft2.tx_to 
                  AND ft1.tx_to = ft2.tx_from
                  AND ft1.token_bought_address = ft2.token_sold_address
                  AND ft1.token_sold_address = ft2.token_bought_address
            )
      )
),

-- Filter 3: Suspicious volume patterns (same wallet, high volume, minimal value extracted)
filter_3_suspicious_volume AS (
    SELECT tx_hash, true AS suspicious_volume
    FROM (
        SELECT 
            wt.tx_hash,
            COUNT(*) as trade_count,
            SUM(wt.amount_usd) as total_volume,
            SUM(CASE WHEN wt.tx_from = wt.wallet_addr THEN wt.amount_usd ELSE CAST(0.0 AS DECIMAL(38,18)) END) as sell_volume,
            SUM(CASE WHEN wt.tx_to = wt.wallet_addr THEN wt.amount_usd ELSE CAST(0.0 AS DECIMAL(38,18)) END) as buy_volume
        FROM (
            SELECT tt.tx_hash, tt.tx_from as wallet_addr, tt.tx_from, tt.tx_to, tt.amount_usd 
            FROM thin_trades tt
            INNER JOIN tx_basic_candidates tbc ON tt.tx_hash = tbc.tx_hash
            UNION ALL
            SELECT tt.tx_hash, tt.tx_to as wallet_addr, tt.tx_from, tt.tx_to, tt.amount_usd 
            FROM thin_trades tt
            INNER JOIN tx_basic_candidates tbc ON tt.tx_hash = tbc.tx_hash
        ) wt
        GROUP BY wt.tx_hash, wt.wallet_addr
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
            tt.tx_hash,
            COUNT(DISTINCT tt.tx_from) as unique_senders,
            COUNT(DISTINCT tt.tx_to) as unique_receivers,
            COUNT(*) as total_trades
        FROM thin_trades tt
        INNER JOIN tx_basic_candidates tbc ON tt.tx_hash = tbc.tx_hash
        GROUP BY tt.tx_hash
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
            tt.tx_hash,
            COUNT(DISTINCT tt.token_bought_address) as unique_tokens,
            COUNT(*) as total_trades,
            -- Check for artificial volume creation on low-liquidity tokens
            AVG(tt.amount_usd) as avg_trade_size,
            MAX(tt.amount_usd) as max_trade_size,
            MIN(tt.amount_usd) as min_trade_size
        FROM thin_trades tt
        INNER JOIN tx_basic_candidates tbc ON tt.tx_hash = tbc.tx_hash
        GROUP BY tt.tx_hash
    ) subq
    WHERE unique_tokens <= 2  -- Focus on 1-2 tokens
    AND total_trades >= 8     -- High frequency
    AND max_trade_size / COALESCE(NULLIF(min_trade_size, 0.0), 1.0) > 100.0  -- Suspicious size variation
    AND avg_trade_size > 10000.0  -- Significant average size
),

-- OPTIMIZATION: only process candidate transactions downstream
wash_candidate_tx AS (
    SELECT tx_hash FROM filter_1_self_trading
    UNION
    SELECT tx_hash FROM filter_2_circular_same_tx
    UNION
    SELECT tx_hash FROM filter_3_suspicious_volume
    UNION
    SELECT tx_hash FROM filter_4_related_wallets
    UNION
    SELECT tx_hash FROM filter_5_token_manipulation
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
    END AS is_wash_trade

FROM filtered_trades dt
INNER JOIN wash_candidate_tx wct ON dt.tx_hash = wct.tx_hash
LEFT JOIN filter_1_self_trading f1 ON dt.tx_hash = f1.tx_hash
LEFT JOIN filter_2_circular_same_tx f2 ON dt.tx_hash = f2.tx_hash
LEFT JOIN filter_3_suspicious_volume f3 ON dt.tx_hash = f3.tx_hash
LEFT JOIN filter_4_related_wallets f4 ON dt.tx_hash = f4.tx_hash
LEFT JOIN filter_5_token_manipulation f5 ON dt.tx_hash = f5.tx_hash

{% endmacro %}