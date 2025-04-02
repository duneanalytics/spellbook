{% macro 
    balancer_v2_compatible_token_balance_changes_macro(
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

    swaps_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            token,
            SUM(COALESCE(delta, INT256 '0')) AS delta
        FROM
            (
                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    poolId AS pool_id,
                    tokenIn AS token,
                    CAST(amountIn as int256) AS delta
                FROM {{ source(project_decoded_as ~ '_' ~ blockchain, 'Vault_evt_Swap') }}
                {% if is_incremental() %}
                WHERE {{ incremental_predicate('evt_block_time') }}
                {% endif %}

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    poolId AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM {{ source(project_decoded_as ~ '_' ~ blockchain, 'Vault_evt_Swap') }}
                {% if is_incremental() %}
                WHERE {{ incremental_predicate('evt_block_time') }}
                {% endif %}
            ) swaps
        GROUP BY 1, 2, 3, 4, 5, 6
    ),

    zipped_balance_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            poolId AS pool_id,
            t.tokens,
            d.deltas,
            p.protocolFeeAmounts
        FROM {{ source(project_decoded_as ~ '_' ~ blockchain, 'Vault_evt_PoolBalanceChanged') }}
        CROSS JOIN UNNEST (tokens) WITH ORDINALITY as t(tokens,i)
        CROSS JOIN UNNEST (deltas) WITH ORDINALITY as d(deltas,i)
        CROSS JOIN UNNEST (protocolFeeAmounts) WITH ORDINALITY as p(protocolFeeAmounts,i)
        WHERE t.i = d.i 
        AND d.i = p.i
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    ),

    balances_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            tokens AS token,
            deltas - CAST(protocolFeeAmounts as int256) AS delta
        FROM zipped_balance_changes
    ),

    managed_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            poolId AS pool_id,
            token,
            cashDelta + managedDelta AS delta
        FROM {{ source(project_decoded_as ~ '_' ~ blockchain, 'Vault_evt_PoolBalanceManaged') }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}
    )


        SELECT
            date_trunc('day', b.evt_block_time) AS block_date,
            b.evt_block_time,
            b.evt_block_number,
            '{{blockchain}}' AS blockchain,
            b.evt_tx_hash,
            b.evt_index,
            b.pool_id,
            BYTEARRAY_SUBSTRING(b.pool_id, 1, 20) AS pool_address,
            p.pool_symbol,
            p.pool_type,
            '{{version}}' AS version, 
            b.token AS token_address,
            t.symbol AS token_symbol,
            b.amount AS delta_amount_raw,
            CASE WHEN BYTEARRAY_SUBSTRING(b.pool_id, 1, 20) = b.token
            THEN amount / POWER (10, 18) --for Balancer Pool Tokens
            ELSE amount / POWER (10, COALESCE(t.decimals, 0)) 
            END AS delta_amount
        FROM
            (
                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    COALESCE(delta, INT256 '0') AS amount
                FROM balances_changes

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    CAST(delta AS int256) AS amount
                FROM managed_changes
            ) b
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token
        AND blockchain = '{{blockchain}}'
        LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}

    {% endmacro %}

{# ######################################################################### #}

{% macro 
    balancer_v3_compatible_token_balance_changes_macro(
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
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            token,
            SUM(COALESCE(delta, INT256 '0')) AS delta
        FROM
            (
                SELECT
                    swap.evt_block_time,
                    swap.evt_block_number,
                    swap.evt_tx_hash,
                    swap.evt_index,
                    swap.pool AS pool_id,
                    swap.tokenIn AS token,
                    CAST(swap.amountIn AS INT256) - (CAST(swap.swapFeeAmount AS INT256) * (g.global_swap_fee + COALESCE(pc.pool_creator_swap_fee, 0))) AS delta
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }} swap
                CROSS JOIN global_fees g
                LEFT JOIN pool_creator_fees pc ON swap.pool = pc.pool AND pc.rn = 1
                WHERE g.rn = 1

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool AS pool_id,
                    tokenOut AS token,
                    -CAST(amountOut AS int256) AS delta
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_Swap') }}
            ) swaps
        GROUP BY 1, 2, 3, 4, 5, 6
    ),

    balance_changes AS(
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            category,
            deltas,
            swapFeeAmountsRaw
        FROM
            (
                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool AS pool_id,
                    'add' AS category,
                    amountsAddedRaw AS deltas,
                    swapFeeAmountsRaw
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_LiquidityAdded') }}

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool AS pool_id,
                    'remove' AS category,
                    amountsRemovedRaw AS deltas,
                    swapFeeAmountsRaw
                FROM {{ source(project_decoded_as + '_' + blockchain, 'Vault_evt_LiquidityRemoved') }}
            ) adds_and_removes
    ),

    zipped_balance_changes AS (
        SELECT
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
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
            evt_block_time,
            evt_block_number,
            evt_tx_hash,
            evt_index,
            pool_id,
            tokens AS token,
            deltas - CAST(swapFeeAmountsRaw as int256) AS delta
        FROM zipped_balance_changes
        ORDER BY 1, 2, 3
    )


        SELECT
            date_trunc('day', b.evt_block_time) AS block_date,
            b.evt_block_time,
            b.evt_block_number,
            '{{blockchain}}' AS blockchain,
            b.evt_tx_hash,
            b.evt_index,
            b.pool_id,
            BYTEARRAY_SUBSTRING(b.pool_id, 1, 20) AS pool_address,
            p.pool_symbol,
            p.pool_type,
            '{{version}}' AS version, 
            b.token AS token_address,
            t.symbol AS token_symbol,
            b.amount AS delta_amount_raw,
            CASE WHEN BYTEARRAY_SUBSTRING(b.pool_id, 1, 20) = b.token
            THEN amount / POWER (10, 18) --for Balancer Pool Tokens
            ELSE amount / POWER (10, COALESCE(t.decimals, 0)) 
            END AS delta_amount
        FROM
            (
                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    COALESCE(delta, INT256 '0') AS amount
                FROM balances_changes

                UNION ALL

                SELECT
                    evt_block_time,
                    evt_block_number,
                    evt_tx_hash,
                    evt_index,
                    pool_id,
                    token,
                    delta AS amount
                FROM
                    swaps_changes
            ) b
        LEFT JOIN {{ source('tokens', 'erc20') }} t ON t.contract_address = b.token
        AND blockchain = '{{blockchain}}'
        LEFT JOIN pool_labels p ON p.pool_id = BYTEARRAY_SUBSTRING(b.pool_id, 1, 20)
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}

    {% endmacro %}