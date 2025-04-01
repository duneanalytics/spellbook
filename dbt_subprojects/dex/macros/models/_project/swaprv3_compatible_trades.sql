{% macro swaprv3_compatible_trades(
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
WITH dexs AS (
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
        CASE 
            WHEN t.amount0 < INT256 '0' THEN f.token0 
            ELSE f.token1 
        END AS token_bought_address,
        CASE 
            WHEN t.amount0 < INT256 '0' THEN f.token1 
            ELSE f.token0 
        END AS token_sold_address,
        t.contract_address AS project_contract_address,
        -- Pull the fee from the fee events table (if available)
        fe.fee,
        t.evt_tx_hash AS tx_hash,
        t.evt_index AS evt_index
    FROM {{ Pair_evt_Swap }} t
    INNER JOIN {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    LEFT JOIN {{ Fee_evt }} fe
        ON fe.contract_address = t.contract_address
           AND fe.evt_tx_hash = t.evt_tx_hash
           AND fe.evt_index = t.evt_index
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('t.evt_block_time') }}
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
    CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index,
    dexs.fee
FROM dexs
{% endmacro %}