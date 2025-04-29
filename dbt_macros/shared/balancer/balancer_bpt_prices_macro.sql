{% macro
    balancer_v2_compatible_bpt_prices_macro(
        blockchain, version, project_decoded_as, base_spells_namespace, pool_labels_spell
    )
%}

WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM {{ pool_labels_spell }}
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
        FROM {{ source('gyroscope','gyro_tokens') }}
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
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }}

                UNION ALL

                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }}
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
        FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolBalanceChanged') }}
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
        FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolBalanceManaged') }}
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
        LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.protocol_liquidity_usd > 0
        LEFT JOIN {{ source('balancer','token_whitelist') }} q ON b.token = q.address
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
    LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '{{version}}'
    AND w.token_address = c.token
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)
    GROUP BY 1, 2, 3, 4
    ),

-- trade based formulation, for Linear Pools (former BPT prices spell)

    bpt_trades AS (
        SELECT *
        FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }} v
        LEFT JOIN pool_labels l ON bytearray_substring(v.poolId, 1, 20) = l.pool_id
        WHERE v.tokenIn = bytearray_substring(v.poolId, 1, 20) OR v.tokenOut = bytearray_substring(v.poolId, 1, 20)
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
            COALESCE(p1.decimals, 18) AS token_in_decimals,
            p2.price AS token_out_p,
            COALESCE(p2.decimals, 18) AS token_out_decimals
        FROM bpt_trades a
        LEFT JOIN prices p1 ON p1.token = a.tokenIn
        AND  p1.day = date_trunc('day', a.evt_block_time)
        LEFT JOIN prices p2 ON p2.token = a.tokenOut
        AND  p2.day = date_trunc('day', a.evt_block_time)
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

    backfill_pricing AS (
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
        FROM all_trades_calc_2
    ),

    trade_price_formulation AS (
        SELECT
            date_trunc('day', block_time) AS day,
            contract_address,
            approx_percentile(price, 0.5) FILTER (WHERE is_finite(price)) AS median_price
        FROM (
            SELECT block_time, contract_address, token_in_price AS price
            FROM backfill_pricing b2 WHERE b2.contract_address = b2.token_in
            UNION
            SELECT block_time, contract_address, token_out_price AS price
            FROM backfill_pricing b2 WHERE b2.contract_address = b2.token_out
        )
        GROUP BY 1, 2
    ),

    trade_price_formulation_2 AS(
        SELECT
            day,
            contract_address,
            approx_percentile(median_price, 0.5) OVER(
                        PARTITION BY contract_address ORDER BY day
                        ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
                    ) AS median_price
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
        CASE WHEN median_price IS NOT NULL
        THEN p.median_price
        ELSE l.liquidity / s.supply
        END AS bpt_price
    FROM tvl l
    LEFT JOIN {{ ref(base_spells_namespace + '_bpt_supply') }} s ON l.pool_address = s.token_address
    AND l.blockchain = s.blockchain
    AND l.version = s.version
    AND l.day = s.day
    LEFT JOIN price_formulation p ON p.day = l.day AND p.contract_address = l.pool_address
    LEFT JOIN pool_labels pl ON pl.pool_id = l.pool_address
    WHERE supply > 0

    {% endmacro %}

    {# ######################################################################### #}

 {% macro
    balancer_v3_compatible_bpt_prices_macro(
        blockchain, version, project_decoded_as, base_spells_namespace, pool_labels_spell
    )
%}

WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM {{ pool_labels_spell }}
        WHERE blockchain = '{{blockchain}}'
    ),

    token_data AS (
        SELECT
            pool,
            ARRAY_AGG(FROM_HEX(json_extract_scalar(token, '$.token')) ORDER BY token_index) AS tokens 
        FROM (
            SELECT
                pool,
                tokenConfig,
                SEQUENCE(1, CARDINALITY(tokenConfig)) AS token_index_array
            FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_PoolRegistered') }}
        ) AS pool_data
        CROSS JOIN UNNEST(tokenConfig, token_index_array) AS t(token, token_index)
        GROUP BY 1
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

    erc4626_prices AS (
        SELECT
            DATE_TRUNC('day', minute) AS day,
            wrapped_token AS token,
            decimals,
            APPROX_PERCENTILE(median_price, 0.5) AS price,
            LEAD(DATE_TRUNC('day', minute), 1, CURRENT_DATE + INTERVAL '1' day) OVER (PARTITION BY wrapped_token ORDER BY date_trunc('day', minute)) AS next_change
        FROM {{ source('balancer_v3' , 'erc4626_token_prices') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),

    global_fees AS (
        SELECT
            evt_block_time,
            swapFeePercentage / 1e18 AS global_swap_fee,
            ROW_NUMBER() OVER (ORDER BY evt_block_time DESC) AS rn
        FROM {{ source(project_decoded_as + '_' + blockchain, 'ProtocolFeeController_evt_GlobalProtocolSwapFeePercentageChanged') }}
    ),

    pool_creator_fees AS (
        SELECT
            evt_block_time,
            pool,
            poolCreatorSwapFeePercentage / 1e18 AS pool_creator_swap_fee,
            ROW_NUMBER() OVER (PARTITION BY pool ORDER BY evt_block_time DESC) AS rn
        FROM {{ source(project_decoded_as + '_' + blockchain, 'ProtocolFeeController_evt_PoolCreatorSwapFeePercentageChanged') }}
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
                    date_trunc('day', swap.evt_block_time) AS day,
                    swap.pool AS pool_id,
                    swap.tokenIn AS token,
                    CAST(swap.amountIn AS INT256) - (CAST(swap.swapFeeAmount AS INT256) * (g.global_swap_fee + COALESCE(pc.pool_creator_swap_fee, 0))) AS delta
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }} swap
                CROSS JOIN global_fees g
                LEFT JOIN pool_creator_fees pc ON swap.pool = pc.pool AND pc.rn = 1
                WHERE g.rn = 1

                UNION ALL

                SELECT
                    date_trunc('day', evt_block_time) AS day,
                    pool AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS INT256) AS delta
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }}
            ) swaps
        GROUP BY 1, 2, 3
    ),

    balance_changes AS(
        SELECT
            evt_block_time,
            pool_id,
            category,
            deltas,
            swapFeeAmountsRaw
        FROM
            (
                SELECT
                    evt_block_time,
                    pool AS pool_id,
                    'add' AS category,
                    amountsAddedRaw AS deltas,
                    swapFeeAmountsRaw
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_LiquidityAdded') }}

                UNION ALL

                SELECT
                    evt_block_time,
                    pool AS pool_id,
                    'remove' AS category,
                    amountsRemovedRaw AS deltas,
                    swapFeeAmountsRaw
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_LiquidityRemoved') }}
            ) adds_and_removes
    ),

    zipped_balance_changes AS (
        SELECT
            date_trunc('day', evt_block_time) AS day,
            pool_id,
            t.tokens,
            CASE WHEN b.category = 'add'
            THEN d.deltas
            WHEN b.category = 'remove'
            THEN -d.deltas
            END AS deltas,
            p.swapFeeAmountsRaw
        FROM balance_changes b
        JOIN token_data td ON b.pool_id = td.pool
        CROSS JOIN UNNEST (td.tokens) WITH ORDINALITY as t(tokens,i)
        CROSS JOIN UNNEST (b.deltas) WITH ORDINALITY as d(deltas,i)
        CROSS JOIN UNNEST (b.swapFeeAmountsRaw) WITH ORDINALITY as p(swapFeeAmountsRaw,i)
        WHERE t.i = d.i
        AND d.i = p.i
        ORDER BY 1,2,3
    ),

    balances_changes AS (
        SELECT
            day,
            pool_id,
            tokens AS token,
            deltas - CAST(swapFeeAmountsRaw as int256) AS delta
        FROM zipped_balance_changes
        ORDER BY 1, 2, 3
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
        FROM unnest(sequence(date('2024-12-01'), date(now()), interval '1' day)) as t(date_sequence)
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
        LEFT JOIN erc4626_prices p4 ON p4.day <= c.day
        AND c.day < p4.next_change
        AND p4.token = b.token
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
        LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.protocol_liquidity_usd > 0
        LEFT JOIN {{ source('balancer','token_whitelist') }} q ON b.token = q.address
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
    LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '{{version}}'
    AND w.token_address = c.token
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)
    GROUP BY 1, 2, 3, 4
    )

    SELECT
        l.day,
        l.blockchain,
        l.version,
        18 AS decimals,
        l.pool_address AS contract_address,
        pl.pool_type,
        l.liquidity / s.supply AS bpt_price
    FROM tvl l
    LEFT JOIN {{ ref(base_spells_namespace + '_bpt_supply') }} s ON l.pool_address = s.token_address
    AND l.blockchain = s.blockchain
    AND l.version = s.version
    AND l.day = s.day
    LEFT JOIN pool_labels pl ON pl.pool_id = l.pool_address
    WHERE supply > 0

    {% endmacro %}   
