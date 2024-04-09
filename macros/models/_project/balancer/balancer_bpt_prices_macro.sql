{% macro 
    balancer_bpt_prices_macro(
        blockchain, version
    ) 
%}

WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM {{ ref('labels_balancer_v2_pools') }}
        WHERE blockchain = '{{blockchain}}'
    ),

-- liquidity formulation, with a few simplifications, compared to liquidity spell

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),

    gyro_prices AS (
        SELECT
            token_address,
            decimals,
            price
        FROM {{ ref('gyroscope_gyro_tokens') }}
        WHERE blockchain = '{{blockchain}}'
    ),

    swaps_changes AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(delta, INT256 '0')) AS delta
        FROM
            (
                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenIn AS token,
                    CAST(amountIn as int256) AS delta
                FROM {{ source('balancer_v2_' ~ blockchain, 'Vault_evt_Swap') }}

                UNION ALL

                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM {{ source('balancer_v2_' ~ blockchain, 'Vault_evt_Swap') }}
            ) swaps
        GROUP BY 1, 2, 3
    ),

    zipped_balance_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            t.tokens,
            d.deltas,
            p.protocolFeeAmounts
        FROM {{ source('balancer_v2_' ~ blockchain, 'Vault_evt_PoolBalanceChanged') }}
        CROSS JOIN UNNEST (tokens) WITH ORDINALITY as t(tokens,i)
        CROSS JOIN UNNEST (deltas) WITH ORDINALITY as d(deltas,i)
        CROSS JOIN UNNEST (protocolFeeAmounts) WITH ORDINALITY as p(protocolFeeAmounts,i)
        WHERE t.i = d.i 
        AND d.i = p.i
        ORDER BY 1,2,3
    ),

    balances_changes AS (
        SELECT
            day,
            pool_id,
            tokens AS token,
            deltas - CAST(protocolFeeAmounts as int256) AS delta
        FROM zipped_balance_changes
        ORDER BY 1, 2, 3
    ),

    managed_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            poolId AS pool_id,
            token,
            cashDelta + managedDelta AS delta
        FROM {{ source('balancer_v2_' ~ blockchain, 'Vault_evt_PoolBalanceManaged') }}
    ),

    daily_delta_balance AS (
        SELECT
            day,
            pool_id,
            token,
            SUM(COALESCE(amount, INT256 '0')) AS amount
        FROM
            (
                SELECT
                    day,
                    pool_id,
                    token,
                    SUM(COALESCE(delta, INT256 '0')) AS amount
                FROM balances_changes
                GROUP BY 1, 2, 3

                UNION ALL

                SELECT
                    day,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes

                UNION ALL

                SELECT
                    day,
                    pool_id,
                    token,
                    CAST(delta AS int256) AS amount
                FROM managed_changes
            ) balance
        GROUP BY 1, 2, 3
    ),

    cumulative_balance AS (
        SELECT
            DAY,
            pool_id,
            token,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token, pool_id ORDER BY DAY) AS day_of_next_change,
            SUM(amount) OVER (PARTITION BY pool_id, token ORDER BY DAY ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount
        FROM daily_delta_balance
    ),

    calendar AS (
        SELECT date_sequence AS day
        FROM unnest(sequence(date('2021-04-21'), date(now()), interval '1' day)) as t(date_sequence)
    ),

   cumulative_usd_balance AS (
        SELECT
            c.day,
            '{{blockchain}}' as blockchain,
            b.pool_id,
            b.token,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p4.decimals)) * COALESCE(p1.price, p4.price, 0) AS protocol_liquidity_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token
        AND blockchain = '{{blockchain}}'
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN gyro_prices p4 ON p4.token_address = b.token
        WHERE b.token != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    weighted_pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            q.name,
            pool_type,
            ROW_NUMBER() OVER (partition by b.day, b.pool_id ORDER BY SUM(b.protocol_liquidity_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.protocol_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1) AS protocol_liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN {{ ref('balancer_pools_tokens_weights') }} w ON b.pool_id = w.pool_id 
        AND b.token = w.token_address
        AND b.protocol_liquidity_usd > 0
        LEFT JOIN {{ ref('balancer_token_whitelist') }} q ON b.token = q.address 
        AND b.blockchain = q.chain
        LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
        WHERE q.name IS NOT NULL 
        AND p.pool_type IN ('weighted') -- filters for weighted pools with pricing assets
        AND w.blockchain = '{{blockchain}}'
        AND w.version = '{{version}}'
        GROUP BY 1, 2, 3, 4
    ),
    
    weighted_pool_liquidity_estimates_2 AS(
    SELECT  e.day,
            e.pool_id,
            SUM(e.protocol_liquidity) / MAX(e.pricing_count) AS protocol_liquidity
    FROM weighted_pool_liquidity_estimates e
    GROUP BY 1,2
    ),
    
    tvl AS(
    SELECT
        c.day,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        '{{version}}' AS version,
        '{{blockchain}}' AS blockchain,
        SUM(COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd)) AS liquidity
    FROM cumulative_usd_balance c
    FULL OUTER JOIN weighted_pool_liquidity_estimates_2 b ON c.day = b.day
    AND c.pool_id = b.pool_id
    LEFT JOIN {{ ref('balancer_pools_tokens_weights') }} w ON b.pool_id = w.pool_id 
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '{{version}}'    
    AND w.token_address = c.token
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)
    GROUP BY 1, 2, 3, 4
    ),

-- trade based formulation, for Linear Pools (former BPT prices spell)

    bpt_trades AS (
        SELECT * 
        FROM {{ source('balancer_v2_' ~ blockchain, 'Vault_evt_Swap') }} v
        LEFT JOIN pool_labels l ON bytearray_substring(v.poolId, 1, 20) = l.pool_id
        WHERE v.tokenIn = bytearray_substring(v.poolId, 1, 20) OR v.tokenOut = bytearray_substring(v.poolId, 1, 20)
        AND l.pool_type = 'linear'
    ), 

    all_trades_info AS (
        SELECT
            a.evt_tx_hash AS tx_hash,
            a.evt_block_time AS block_time,
            a.evt_block_number AS block_number,
            a.poolId AS pool_id,
            bytearray_substring(a.poolId, 1, 20) AS bpt_address,
            a.tokenIn AS token_in,
            CAST(a.amountIn AS DOUBLE) AS amount_in,
            a.tokenOut AS token_out,
            CAST(a.amountOut AS DOUBLE) AS amount_out,
            p1.price AS token_in_p,
            COALESCE(p1.symbol, t1.symbol) AS token_in_sym,
            COALESCE(p1.decimals, t1.decimals) AS token_in_decimals,
            p2.price AS token_out_p,
            COALESCE(p2.symbol, t2.symbol) AS token_out_sym,
            COALESCE(p2.decimals, t2.decimals) AS token_out_decimals
        FROM bpt_trades a
        LEFT JOIN {{ source ('prices', 'usd') }} p1 ON p1.contract_address = a.tokenIn AND p1.blockchain = '{{blockchain}}'
        AND  p1.minute = date_trunc('minute', a.evt_block_time)
        LEFT JOIN {{ source ('prices', 'usd') }} p2 ON p2.contract_address = a.tokenOut AND p2.blockchain = '{{blockchain}}'
        AND  p2.minute = date_trunc('minute', a.evt_block_time)
        LEFT JOIN {{ source('tokens', 'erc20') }} t1 ON t1.contract_address = a.tokenIn AND t1.blockchain = '{{blockchain}}'
        LEFT JOIN {{ source('tokens', 'erc20') }} t2 ON t2.contract_address = a.tokenOut AND t2.blockchain = '{{blockchain}}'
        ORDER BY a.evt_block_number DESC, a.evt_index DESC
    ),

    all_trades_calc_2 AS (
        SELECT *,
            amount_in / POWER(10, COALESCE(token_in_decimals, 18)) AS amount_in_norm,
            amount_out / POWER(10, COALESCE(token_out_decimals, 18)) AS amount_out_norm,
            (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) / (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) AS in_out_norm_rate,
            (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) / (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) AS out_in_norm_rate,
            CASE
                WHEN token_in_p IS NULL AND token_out_p IS NULL THEN NULL
                ELSE COALESCE(
                        token_in_p,
                        (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) / (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) * token_out_p
                    )
            END AS token_in_price,
            CASE
                WHEN token_in_p IS NULL AND token_out_p IS NULL THEN NULL
                ELSE COALESCE(
                        token_out_p,
                        (amount_in / POWER(10, COALESCE(token_in_decimals, 18))) / (amount_out / POWER(10, COALESCE(token_out_decimals, 18))) * token_in_p
                    )
            END AS token_out_price
        FROM all_trades_info
    ),

    unique_tx_token_price AS (
        SELECT
            distinct
                tx_hash,
                token,
                AVG(token_price) OVER(PARTITION BY tx_hash, token) AS avg_price
        FROM (
            SELECT tx_hash, token_in AS token, token_in_price AS token_price
            FROM all_trades_calc_2
            UNION ALL
            SELECT tx_hash, token_out AS token, token_out_price AS token_price
            FROM all_trades_calc_2
        )

        ORDER BY 1,2
    ),

    backfill_pricing_1 AS (
        SELECT
            c2.block_time,
            c2.tx_hash,
            c2.bpt_address,
            c2.token_in,
            c2.in_out_norm_rate,
            COALESCE(c2.token_in_price, u1.avg_price) AS token_in_price,
            c2.token_out,
            c2.out_in_norm_rate,
            COALESCE(c2.token_out_price, u2.avg_price) AS token_out_price
        FROM all_trades_calc_2 c2
        LEFT JOIN unique_tx_token_price u1 ON u1.tx_hash = c2.tx_hash AND u1.token = c2.token_in
        LEFT JOIN unique_tx_token_price u2 ON u2.tx_hash = c2.tx_hash AND u2.token = c2.token_out
    ),

    backfill_pricing_2 AS (
        SELECT
            block_time,
            tx_hash,
            bpt_address AS contract_address,
            token_in,
            COALESCE(token_in_price, (out_in_norm_rate * token_out_price)) AS token_in_price,
            token_out,
            COALESCE(token_out_price, (in_out_norm_rate * token_in_price)) AS token_out_price,
            in_out_norm_rate,
            out_in_norm_rate
        FROM backfill_pricing_1
    ),

    trade_price_formulation AS (
        SELECT
            date_trunc('day', block_time) AS day,
            contract_address,
            approx_percentile(price, 0.5) FILTER (WHERE is_finite(price)) AS median_price
        FROM (
            SELECT block_time, contract_address, token_in_price AS price
            FROM backfill_pricing_2 b2 WHERE b2.contract_address = b2.token_in
            UNION
            SELECT block_time, contract_address, token_out_price AS price
            FROM backfill_pricing_2 b2 WHERE b2.contract_address = b2.token_out
        )
        GROUP BY 1, 2
    ),

    trade_price_formulation_2 AS(
        SELECT
            day,
            contract_address,
            CASE
                WHEN median_price IS NOT NULL THEN median_price
                WHEN LEAD(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC) IS NOT NULL
                THEN LEAD(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC)
                WHEN LAG(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC) IS NOT NULL
                THEN LAG(median_price) OVER(PARTITION BY contract_address ORDER BY day DESC)
                ELSE approx_percentile(median_price, 0.5) OVER(
                        PARTITION BY contract_address ORDER BY day
                        ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
                    )
            END AS median_price
        FROM trade_price_formulation
    ),

    price_formulation AS(
        SELECT
            day,
            contract_address,
            median_price
        FROM (
            SELECT
                day,
                contract_address,
                median_price,
                AVG(median_price) OVER (PARTITION BY contract_address) AS avg_median_price
            FROM trade_price_formulation
        ) subquery
        WHERE median_price < avg_median_price * 2 --removes outliers
        GROUP BY 1, 2, 3
    )

    SELECT 
        l.day,
        l.blockchain,
        l.version,
        18 AS decimals,
        l.pool_address AS contract_address,
        pl.pool_type,
        CASE WHEN pl.pool_type = 'linear' AND median_price IS NOT NULL
        THEN p.median_price
        WHEN l.liquidity = 0 AND median_price IS NOT NULL 
        THEN p.median_price
        ELSE l.liquidity / s.supply 
        END AS bpt_price
    FROM tvl l
    LEFT JOIN {{ ref('balancer_bpt_supply') }} s ON l.pool_address = s.token_address
    AND l.blockchain = s.blockchain
    AND l.version = s.version  
    AND l.day = s.day
    LEFT JOIN price_formulation p ON p.day = l.day AND p.contract_address = l.pool_address
    LEFT JOIN pool_labels pl ON pl.pool_id = l.pool_address
    WHERE supply > 1

    {% endmacro %}
