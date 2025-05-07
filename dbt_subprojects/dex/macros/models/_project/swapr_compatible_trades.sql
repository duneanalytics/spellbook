{% macro swapr_v3_compatible_trades(
    blockchain = null,
    project = null,
    version = null,
    Pair_evt_Swap = null,
    Factory_evt_PoolCreated = null,
    Fee_evt = null,
    taker_column_name = 'recipient',
    maker_column_name = null,
    optional_columns = [],
    pair_column_name = 'pool'
) %}
WITH fee_events AS (
    SELECT *
    FROM {{ Fee_evt }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
),
base_swaps AS (
    SELECT
        t.evt_block_number AS block_number,
        t.evt_block_time AS block_time,
        t.{{ taker_column_name }} AS taker,
        {% if maker_column_name %}
            t.{{ maker_column_name }}
        {% else %}
            CAST(NULL AS VARBINARY)
        {% endif %} AS maker,
        CASE 
            WHEN t.amount0 < INT256 '0' THEN ABS(t.amount0)
            ELSE ABS(t.amount1)
        END AS token_bought_amount_raw,
        CASE 
            WHEN t.amount0 < INT256 '0' THEN ABS(t.amount1)
            ELSE ABS(t.amount0)
        END AS token_sold_amount_raw,
        f.token0 AS token0,
        f.token1 AS token1,
        t.contract_address AS project_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index AS evt_index,
        t.amount0,
        t.amount1,
        t.liquidity,
        t.price,
        t.sender AS swap_sender
        {% if optional_columns | length > 0 %}
            {%- for col in optional_columns %}
                , {{ col }}
            {%- endfor %}
        {% endif %}
    FROM {{ Pair_evt_Swap }} t
    INNER JOIN {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    {% if is_incremental() %}
      WHERE {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
),
joined_fee AS (
    -- Fee event is emmited when new timepoint appears, only for first swap in block
    -- So there should always be a fee event to forward fill missing values within partition
    SELECT
        bs.*
        ,LAST_VALUE(fe.fee) IGNORE NULLS OVER w AS fee
    FROM base_swaps bs
    LEFT JOIN fee_events fe
        ON fe.evt_tx_hash = bs.tx_hash
        AND fe.contract_address = bs.project_contract_address
        AND fe.evt_index < bs.evt_index
    WINDOW w AS (
        PARTITION BY 
            bs.project_contract_address
        ORDER BY 
            bs.block_time
            ,bs.evt_index
    )
)
SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', block_time) AS date) AS block_month,
    CAST(date_trunc('day', block_time) AS date) AS block_date,
    block_time,
    block_number,
    CAST(token_bought_amount_raw AS UINT256) AS token_bought_amount_raw,
    CAST(token_sold_amount_raw AS UINT256) AS token_sold_amount_raw,
    CASE WHEN amount0 < INT256 '0' THEN token0 ELSE token1 END AS token_bought_address,
    CASE WHEN amount0 < INT256 '0' THEN token1 ELSE token0 END AS token_sold_address,
    taker,
    maker,
    project_contract_address,
    tx_hash,
    evt_index,
    fee
FROM joined_fee


{% endmacro %}