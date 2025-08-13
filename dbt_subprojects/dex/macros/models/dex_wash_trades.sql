{% macro dex_wash_trades(blockchain) %}

WITH filter_1_same_wallet AS (
    -- Same wallet buys and sells to itself
    SELECT tx_hash, true AS same_wallet_trading
    FROM {{ ref('dex_trades') }} dt1
    INNER JOIN {{ ref('dex_trades') }} dt2 
        ON dt1.tx_hash = dt2.tx_hash
        AND dt1.tx_from = dt2.tx_from
        AND dt1.token_bought_address = dt2.token_sold_address
        AND dt1.token_sold_address = dt2.token_bought_address
        AND dt1.blockchain = '{{blockchain}}'
        AND dt2.blockchain = '{{blockchain}}'
    WHERE dt1.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND dt1.block_time >= date_trunc('day', NOW() - interval '7' day)
    {% endif %}
),

filter_2_back_forth AS (
    -- Rapid back-and-forth between two wallets
    SELECT dt1.tx_hash, true AS back_forth_trading
    FROM {{ ref('dex_trades') }} dt1
    INNER JOIN {{ ref('dex_trades') }} dt2 
        ON dt1.tx_from = dt2.tx_to
        AND dt1.tx_to = dt2.tx_from
        AND dt1.token_bought_address = dt2.token_sold_address
        AND dt1.token_sold_address = dt2.token_bought_address
        AND date_diff('second', dt1.block_time, dt2.block_time) BETWEEN -300 AND 300 -- 5 minutes arbitrary threshold
        AND dt1.blockchain = '{{blockchain}}'
        AND dt2.blockchain = '{{blockchain}}'
    WHERE dt1.blockchain = '{{blockchain}}'
    {% if is_incremental() %}
    AND dt1.block_time >= date_trunc('day', NOW() - interval '7' day)
    {% endif %}
),

filter_3_high_frequency AS (
    -- Extremely high frequency loops
    SELECT tx_hash, true AS high_frequency_trading
    FROM (
        SELECT tx_hash, COUNT(*) as swap_count
        FROM {{ ref('dex_trades') }}
        WHERE blockchain = '{{blockchain}}'
        {% if is_incremental() %}
        AND block_time >= date_trunc('day', NOW() - interval '7' day)
        {% endif %}
        GROUP BY tx_hash
    ) t
    WHERE swap_count > 50 -- Assumed threshold for high frequency
),

filter_4_circular_trading AS (
    -- Detect circular trading patterns within same tx
    SELECT tx_hash, true AS circular_trading
    FROM (
        SELECT tx_hash, 
               COUNT(DISTINCT tx_from) as unique_traders,
               COUNT(*) as total_trades
        FROM {{ ref('dex_trades') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY tx_hash
    ) t
    WHERE unique_traders <= 3 AND total_trades >= 5 -- This is an arbitrary threshold
),

filter_5_net_zero_pnl AS (
    -- Net zero/minimal PnL after massive volume using USD amounts
    SELECT tx_hash, true AS net_zero_pnl
    FROM (
        SELECT 
            tx_hash,
            SUM(amount_usd) as total_volume,
            -- Net USD position change per trader in this transaction
            MAX(ABS(net_position_change)) as max_net_change
        FROM (
            SELECT 
                tx_hash,
                trader_address,
                SUM(amount_usd) as total_volume,
                -- Net position change: positive = net buying, negative = net selling
                SUM(CASE 
                    WHEN tx_from = trader_address THEN -amount_usd  -- Selling
                    WHEN tx_to = trader_address THEN amount_usd     -- Buying
                    ELSE 0 
                END) as net_position_change
            FROM {{ ref('dex_trades') }}
            CROSS JOIN (
                SELECT DISTINCT tx_from as trader_address FROM {{ ref('dex_trades') }}
                UNION 
                SELECT DISTINCT tx_to as trader_address FROM {{ ref('dex_trades') }}
            ) traders
            WHERE blockchain = '{{blockchain}}'
            GROUP BY tx_hash, trader_address
        ) trader_positions
        GROUP BY tx_hash, total_volume
    ) t
    WHERE total_volume > 100000 
        AND (max_net_change / total_volume) < 0.002 -- Less than 0.2% net change - this is an arbitrary threshold
)

SELECT 
    dt.blockchain,
    dt.project,
    dt.version,
    dt.block_time,
    dt.block_date,
    dt.block_number,
    dt.tx_hash,
    dt.tx_from,
    dt.tx_to,
    dt.token_bought_address,
    dt.token_sold_address,
    dt.token_bought_symbol,
    dt.token_sold_symbol,
    dt.token_bought_amount,
    dt.token_sold_amount,
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
FROM {{ ref('dex_trades') }} dt
LEFT JOIN filter_1_same_wallet f1 ON dt.tx_hash = f1.tx_hash
LEFT JOIN filter_2_back_forth f2 ON dt.tx_hash = f2.tx_hash
LEFT JOIN filter_3_high_frequency f3 ON dt.tx_hash = f3.tx_hash
LEFT JOIN filter_4_circular_trading f4 ON dt.tx_hash = f4.tx_hash
LEFT JOIN filter_5_net_zero_pnl f5 ON dt.tx_hash = f5.tx_hash
WHERE dt.blockchain = '{{blockchain}}'
{% if is_incremental() %}
AND dt.block_time >= date_trunc('day', NOW() - interval '7' day)
{% endif %}

{% endmacro %}
