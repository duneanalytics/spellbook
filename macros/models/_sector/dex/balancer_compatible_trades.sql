{% macro balancer_compatible_v1_trades(
        blockchain = '',
        project = '',
        version = '',
        project_decoded_as = 'balancer_v1',
        BPool_evt_LOG_SWAP = 'BPool_evt_LOG_SWAP',
        BPool_call_setSwapFee = 'BPool_call_setSwapFee'
    )
%}

WITH 

swap_fees AS (
    SELECT * FROM (
        SELECT
            swaps.contract_address,
            swaps.evt_tx_hash,
            swaps.evt_block_time,
            swaps.evt_index,
            swaps.evt_block_number,
            fees.swapFee,
            ROW_NUMBER() OVER (PARTITION BY swaps.contract_address, evt_tx_hash, evt_index ORDER BY call_block_number DESC) AS rn
        FROM {{ source(project_decoded_as ~ '_' ~ blockchain, BPool_evt_LOG_SWAP) }} swaps
            LEFT JOIN {{ source(project_decoded_as ~ '_' ~ blockchain, BPool_call_setSwapFee) }} fees
                ON fees.contract_address = swaps.contract_address
                AND fees.call_block_number < swaps.evt_block_number
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('swaps.evt_block_time') }}
        {% endif %}
    ) t
    WHERE t.rn = 1
),

dexs AS (
    SELECT
        CAST(NULL AS VARBINARY) AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        tokenOut AS token_bought_address,
        tokenAmountOut AS token_bought_amount_raw,
        tokenIn AS token_sold_address,
        tokenAmountIn AS token_sold_amount_raw,
        swaps.contract_address AS project_contract_address,
        (swapFee / 1e18) AS swap_fee_percentage,
        swaps.evt_block_number AS block_number,
        swaps.evt_block_time AS block_time,
        swaps.evt_tx_hash AS tx_hash,
        swaps.evt_index
    FROM {{ source(project_decoded_as ~ '_' ~ blockchain, BPool_evt_LOG_SWAP) }} swaps
        LEFT JOIN swap_fees fees
            ON fees.evt_tx_hash = swaps.evt_tx_hash
            AND fees.evt_block_number = swaps.evt_block_number
            AND fees.contract_address = swaps.contract_address
            AND fees.evt_index = swaps.evt_index
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('swaps.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index,
    CAST(NULL AS varbinary) AS pool_id,
    CAST(dexs.swap_fee_percentage AS double) AS swap_fee
FROM dexs

{% endmacro %}

{# ######################################################################### #}

{% macro balancer_compatible_v2_trades(
        blockchain = '',
        project = '',
        version = '',
        project_decoded_as = 'balancer_v2',
        Vault_evt_Swap = 'Vault_evt_Swap',
        pools_fees = 'pools_fees'
    )
%}

WITH 

swap_fees AS (
    SELECT * FROM (
        SELECT
            swaps.poolId,
            swaps.evt_tx_hash,
            swaps.evt_index,
            swaps.evt_block_number,
            bytearray_substring(swaps.poolId, 1, 20) AS contract_address,
            fees.swap_fee_percentage,
            ROW_NUMBER() OVER (PARTITION BY poolId, evt_tx_hash, evt_index ORDER BY block_number DESC, index DESC) AS rn
        FROM {{ source(project_decoded_as ~ '_' ~ blockchain, Vault_evt_Swap) }} swaps
        LEFT JOIN {{ ref(project_decoded_as ~ '_' ~ blockchain ~ '_' ~ pools_fees) }} fees
            ON fees.contract_address = bytearray_substring(swaps.poolId, 1, 20)
            AND ARRAY[fees.block_number] || ARRAY[fees.index] < ARRAY[swaps.evt_block_number] || ARRAY[swaps.evt_index]
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('swaps.evt_block_time') }}
        {% endif %}
    ) t
    WHERE t.rn = 1
),

dexs AS (
    SELECT
        swap.evt_block_number AS block_number,
        swap.evt_block_time AS block_time,
        CAST(NULL AS VARBINARY) AS taker,
        CAST(NULL AS VARBINARY) AS maker,
        swap.amountOut AS token_bought_amount_raw,
        swap.amountIn AS token_sold_amount_raw,
        swap.tokenOut AS token_bought_address,
        swap.tokenIn AS token_sold_address,
        swap_fees.contract_address AS project_contract_address,
        swap.poolId AS pool_id,
        swap_fees.swap_fee_percentage / POWER(10, 18) AS swap_fee,
        swap.evt_tx_hash AS tx_hash,
        swap.evt_index
    FROM swap_fees
    INNER JOIN {{ source(project_decoded_as ~ '_' ~ blockchain, Vault_evt_Swap) }} swap
        ON swap.evt_block_number = swap_fees.evt_block_number
        AND swap.evt_tx_hash = swap_fees.evt_tx_hash
        AND swap.evt_index = swap_fees.evt_index
    WHERE swap.tokenIn <> swap_fees.contract_address
        AND swap.tokenOut <> swap_fees.contract_address
        {% if is_incremental() %}
        AND {{ incremental_predicate('swap.evt_block_time') }}
        {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', dexs.block_time) AS date) AS block_month,
    CAST(date_trunc('day', dexs.block_time) AS date) AS block_date,
    dexs.block_time,
    dexs.block_number,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index,
    dexs.pool_id,
    dexs.swap_fee
FROM dexs

{% endmacro %}
