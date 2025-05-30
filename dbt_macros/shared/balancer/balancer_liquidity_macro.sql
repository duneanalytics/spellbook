{% macro
    balancer_v2_compatible_liquidity_macro(
        blockchain, version, project_decoded_as, base_spells_namespace, pool_labels_model
    )
%}

WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM {{ source('labels', pool_labels_model) }}
        WHERE blockchain = '{{blockchain}}'
        AND source = 'query'
        AND model_name = '{{pool_labels_model}}'
    ),

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            APPROX_PERCENTILE(price, 0.5) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', HOUR) AS DAY,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            sum(sample_size) AS sample_size
        FROM {{ source('dex', 'prices') }}
        WHERE blockchain = '{{blockchain}}'
        AND contract_address NOT IN (0x039e2fb66102314ce7b64ce5ce3e5183bc94ad38, 0xde1e704dae0b4051e80dabb26ab6ad6c12262da0, 0x5ddb92a5340fd0ead3987d3661afcd6104c3b757) 
        GROUP BY 1, 2
        HAVING sum(sample_size) > 3
    ),

    dex_prices_2 AS(
        SELECT
            day,
            token,
            price,
            lag(price) OVER(PARTITION BY token ORDER BY day) AS previous_price
        FROM dex_prices_1
    ),

    dex_prices AS (
        SELECT
            day,
            token,
            price,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change
        FROM dex_prices_2
        WHERE (price < previous_price * 1e4 AND price > previous_price / 1e4)
    ),

    bpt_prices AS(
        SELECT DISTINCT
            day,
            contract_address AS token,
            decimals,
            bpt_price
        FROM {{ ref(base_spells_namespace + '_bpt_prices') }}
        WHERE blockchain = '{{blockchain}}'
        AND version = '{{version}}'
    ),

    eth_prices AS (
        SELECT
            DATE_TRUNC('day', minute) as day,
            AVG(price) as eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'ethereum'
        AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        GROUP BY 1
    ),

    gyro_prices AS (
        SELECT
            token_address,
            decimals,
            price
        FROM {{ source('gyroscope', 'gyro_tokens') }}
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
            symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p3.decimals, p4.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p3.decimals, p4.decimals)) * COALESCE(p1.price, p2.price, p4.price, 0) AS protocol_liquidity_usd,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p3.decimals, p4.decimals)) * COALESCE(p1.price, p2.price, p3.bpt_price, p4.price, 0) AS pool_liquidity_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token
        AND blockchain = '{{blockchain}}'
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
        LEFT JOIN bpt_prices p3 ON p3.day = b.day
        AND p3.token = b.token
        LEFT JOIN gyro_prices p4 ON p4.token_address = b.token
        WHERE b.token != BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
    ),

    weighted_pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool_id,
            q.name,
            pool_type,
            ROW_NUMBER() OVER (partition by b.day, b.pool_id ORDER BY SUM(b.pool_liquidity_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.protocol_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1) AS protocol_liquidity,
            SUM(b.pool_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1)  AS pool_liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.pool_liquidity_usd > 0
        LEFT JOIN {{ source('balancer', 'token_whitelist') }} q ON b.token = q.address
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
            SUM(e.pool_liquidity) / MAX(e.pricing_count) AS pool_liquidity,
            SUM(e.protocol_liquidity) / MAX(e.pricing_count) AS protocol_liquidity
    FROM weighted_pool_liquidity_estimates e
    GROUP BY 1,2
    )

    SELECT
        c.day,
        c.pool_id,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        p.pool_symbol,
        '{{version}}' AS version,
        '{{blockchain}}' AS blockchain,
        p.pool_type,
        c.token AS token_address,
        c.token_symbol,
        c.token_balance_raw,
        c.token_balance,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd) AS protocol_liquidity_usd,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd)/e.eth_price AS protocol_liquidity_eth,
        COALESCE(b.pool_liquidity * w.normalized_weight, c.pool_liquidity_usd) AS pool_liquidity_usd,
        COALESCE(b.pool_liquidity * w.normalized_weight, c.pool_liquidity_usd)/e.eth_price AS pool_liquidity_eth
    FROM cumulative_usd_balance c
    FULL OUTER JOIN weighted_pool_liquidity_estimates_2 b ON c.day = b.day
    AND c.pool_id = b.pool_id
    LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '{{version}}'
    AND w.token_address = c.token
    LEFT JOIN eth_prices e ON e.day = c.day
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)
    {% endmacro %}

{# ######################################################################### #}

{% macro
    balancer_v3_compatible_liquidity_macro(
        blockchain, version, project_decoded_as, base_spells_namespace, pool_labels_model
    )
%}

WITH pool_labels AS (
        SELECT
            address AS pool_id,
            name AS pool_symbol,
            pool_type
        FROM {{ source('labels', pool_labels_model) }}
        WHERE blockchain = '{{blockchain}}'
        AND source = 'query'
        AND model_name = '{{pool_labels_model}}'
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

    prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            decimals,
            APPROX_PERCENTILE(price, 0.5) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2, 3
    ),

    dex_prices_1 AS (
        SELECT
            date_trunc('day', timestamp) AS DAY,
            contract_address AS token,
            approx_percentile(price, 0.5) AS price
        FROM {{ source('prices', 'hour') }}
        WHERE blockchain = '{{blockchain}}'
        GROUP BY 1, 2
    ),

    dex_prices_2 AS(
        SELECT
            day,
            token,
            price,
            lag(price) OVER(PARTITION BY token ORDER BY day) AS previous_price
        FROM dex_prices_1
    ),

    dex_prices AS (
        SELECT
            day,
            token,
            price,
            LEAD(DAY, 1, NOW()) OVER (PARTITION BY token ORDER BY DAY) AS day_of_next_change
        FROM dex_prices_2
        WHERE (price < previous_price * 1e4 AND price > previous_price / 1e4)
    ),

    bpt_prices AS(
        SELECT DISTINCT
            day,
            contract_address AS token,
            decimals,
            bpt_price
        FROM {{ ref(base_spells_namespace + '_bpt_prices') }}
        WHERE blockchain = '{{blockchain}}'
        AND version = '{{version}}'
    ),

    eth_prices AS (
        SELECT
            DATE_TRUNC('day', minute) as day,
            AVG(price) as eth_price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'ethereum'
        AND contract_address = 0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2
        GROUP BY 1
    ),

    erc4626_prices AS (
        SELECT
            DATE_TRUNC('day', minute) AS day,
            wrapped_token AS token,
            decimals,
            APPROX_PERCENTILE(median_price, 0.5) AS price,
            LEAD(DATE_TRUNC('day', minute), 1, CURRENT_DATE + INTERVAL '1' day) OVER (PARTITION BY wrapped_token ORDER BY date_trunc('day', minute)) AS next_change
        FROM {{ source('balancer_v3', 'erc4626_token_prices') }}
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
            symbol AS token_symbol,
            cumulative_amount as token_balance_raw,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p3.decimals, p4.decimals)) AS token_balance,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p3.decimals, p4.decimals)) * COALESCE(p1.price, p2.price, p4.price, 0) AS protocol_liquidity_usd,
            cumulative_amount / POWER(10, COALESCE(t.decimals, p1.decimals, p3.decimals, p4.decimals)) * COALESCE(p1.price, p2.price, p3.bpt_price, p4.price, 0) AS pool_liquidity_usd
        FROM calendar c
        LEFT JOIN cumulative_balance b ON b.day <= c.day
        AND c.day < b.day_of_next_change
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token
        AND blockchain = '{{blockchain}}'
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= c.day
        AND c.day < p2.day_of_next_change
        AND p2.token = b.token
        LEFT JOIN bpt_prices p3 ON p3.day = b.day
        AND p3.token = b.token
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
            ROW_NUMBER() OVER (partition by b.day, b.pool_id ORDER BY SUM(b.pool_liquidity_usd) ASC) AS pricing_count, --to avoid double count in pools with multiple pricing assets
            SUM(b.protocol_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1) AS protocol_liquidity,
            SUM(b.pool_liquidity_usd) / COALESCE(SUM(w.normalized_weight), 1)  AS pool_liquidity
        FROM cumulative_usd_balance b
        LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
        AND b.token = w.token_address
        AND b.pool_liquidity_usd > 0
        LEFT JOIN {{ source('balancer', 'token_whitelist') }} q ON b.token = q.address
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
            SUM(e.pool_liquidity) / MAX(e.pricing_count) AS pool_liquidity,
            SUM(e.protocol_liquidity) / MAX(e.pricing_count) AS protocol_liquidity
    FROM weighted_pool_liquidity_estimates e
    GROUP BY 1,2
    )

    SELECT
        c.day,
        c.pool_id,
        BYTEARRAY_SUBSTRING(c.pool_id, 1, 20) AS pool_address,
        p.pool_symbol,
        '{{version}}' AS version,
        '{{blockchain}}' AS blockchain,
        p.pool_type,
        c.token AS token_address,
        c.token_symbol,
        c.token_balance_raw,
        c.token_balance,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd) AS protocol_liquidity_usd,
        COALESCE(b.protocol_liquidity * w.normalized_weight, c.protocol_liquidity_usd)/e.eth_price AS protocol_liquidity_eth,
        COALESCE(b.pool_liquidity * w.normalized_weight, c.pool_liquidity_usd) AS pool_liquidity_usd,
        COALESCE(b.pool_liquidity * w.normalized_weight, c.pool_liquidity_usd)/e.eth_price AS pool_liquidity_eth
    FROM cumulative_usd_balance c
    FULL OUTER JOIN weighted_pool_liquidity_estimates_2 b ON c.day = b.day
    AND c.pool_id = b.pool_id
    LEFT JOIN {{ ref(base_spells_namespace + '_pools_tokens_weights') }} w ON b.pool_id = w.pool_id
    AND w.blockchain = '{{blockchain}}'
    AND w.version = '{{version}}'
    AND w.token_address = c.token
    LEFT JOIN eth_prices e ON e.day = c.day
    LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(c.pool_id, 1, 20)

    {% endmacro %}