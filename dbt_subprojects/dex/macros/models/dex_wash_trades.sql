{% macro dex_wash_trades(blockchain) %}

WITH filtered_trades AS (
    SELECT *
    FROM {{ ref('dex_trades') }}
    WHERE blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND block_time >= date_trunc('day', NOW() - interval '7' day)
    {% endif %}
),

filter_1_same_wallet AS (
    -- Same wallet buys and sells the same tokens in same transaction
    SELECT tx_hash, true AS same_wallet_trading
    FROM (
        SELECT 
            tx_hash,
            tx_from,
            -- Check if this wallet both bought and sold the same tokens
            COUNT(CASE WHEN token_bought_address = token_sold_address THEN 1 END) as same_token_trades,
            COUNT(DISTINCT token_bought_address) as unique_tokens_bought,
            COUNT(DISTINCT token_sold_address) as unique_tokens_sold
        FROM filtered_trades
        GROUP BY tx_hash, tx_from
    ) t
    WHERE same_token_trades > 1  -- Multiple trades of same tokens
    AND unique_tokens_bought = unique_tokens_sold  -- Same tokens involved
),

filter_2_back_forth AS (
    -- Rapid back-and-forth between two wallets
    SELECT tx_hash, true AS back_forth_trading
    FROM (
        SELECT 
            tx_hash,
            tx_from,
            tx_to,
            token_bought_address,
            token_sold_address,
            block_time,
            -- Find matching reverse trades within 5 minutes
            LEAD(tx_to) OVER (PARTITION BY tx_from, token_bought_address ORDER BY block_time) as next_tx_to,
            LEAD(tx_from) OVER (PARTITION BY tx_to, token_sold_address ORDER BY block_time) as next_tx_from,
            LEAD(block_time) OVER (PARTITION BY tx_from, token_bought_address ORDER BY block_time) as next_block_time
        FROM filtered_trades
    ) t
    WHERE next_tx_to = tx_from 
    AND next_tx_from = tx_to
    AND date_diff('second', block_time, next_block_time) BETWEEN -300 AND 300
),

filter_3_high_frequency AS (
    -- Extremely high frequency loops
    SELECT tx_hash, true AS high_frequency_trading
    FROM (
        SELECT tx_hash, COUNT(*) as swap_count
        FROM filtered_trades
        GROUP BY tx_hash
    ) t
    WHERE swap_count > 50
),

filter_4_circular_trading AS (
    -- Detect circular trading patterns within same tx
    SELECT tx_hash, true AS circular_trading
    FROM (
        SELECT tx_hash, 
               COUNT(DISTINCT tx_from) as unique_traders,
               COUNT(*) as total_trades
        FROM filtered_trades
        GROUP BY tx_hash
    ) t
    WHERE unique_traders <= 3 AND total_trades >= 5
),

filter_5_net_zero_pnl AS (
    -- Net zero/minimal PnL after massive volume
    SELECT tx_hash, true AS net_zero_pnl
    FROM (
        SELECT 
            tx_hash,
            SUM(amount_usd) as total_volume,
            SUM(CASE 
                WHEN tx_from = tx_to THEN 0  -- Self-trades
                ELSE amount_usd 
            END) as net_position_change
        FROM filtered_trades
        GROUP BY tx_hash
    ) t
    WHERE total_volume > 100000 
    AND (ABS(net_position_change) / total_volume) < 0.002
)

SELECT 
    dt.blockchain,
    dt.project,
    dt.version,
    dt.block_time,
    dt.block_date,
    dt.block_month,
    dt.block_number,
    dt.tx_hash,
    dt.tx_from,
    dt.tx_to,
    dt.taker,
    dt.maker,
    dt.token_bought_address,
    dt.token_sold_address,
    dt.token_bought_symbol,
    dt.token_sold_symbol,
    dt.token_pair,
    dt.token_bought_amount,
    dt.token_sold_amount,
    dt.token_bought_amount_raw,
    dt.token_sold_amount_raw,
    dt.amount_usd,
    dt.project_contract_address,
    dt.evt_index,
    -- Filter flags
    COALESCE(f1.same_wallet_trading, false) AS filter_1_same_wallet,
    COALESCE(f2.back_forth_trading, false) AS filter_2_back_forth,
    COALESCE(f3.high_frequency_trading, false) AS filter_3_high_frequency,
    COALESCE(f4.circular_trading, false) AS filter_4_circular_trading,
    COALESCE(f5.net_zero_pnl, false) AS filter_5_net_zero_pnl,
    -- Combined wash trade flag
    CASE WHEN COALESCE(f1.same_wallet_trading, false) 
              OR COALESCE(f2.back_forth_trading, false)
              OR COALESCE(f3.high_frequency_trading, false)
              OR COALESCE(f4.circular_trading, false)
              OR COALESCE(f5.net_zero_pnl, false)
         THEN true
         ELSE false
    END AS is_wash_trade
FROM filtered_trades dt
LEFT JOIN filter_1_same_wallet f1 ON dt.tx_hash = f1.tx_hash
LEFT JOIN filter_2_back_forth f2 ON dt.tx_hash = f2.tx_hash
LEFT JOIN filter_3_high_frequency f3 ON dt.tx_hash = f3.tx_hash
LEFT JOIN filter_4_circular_trading f4 ON dt.tx_hash = f4.tx_hash
LEFT JOIN filter_5_net_zero_pnl f5 ON dt.tx_hash = f5.tx_hash

{% endmacro %}