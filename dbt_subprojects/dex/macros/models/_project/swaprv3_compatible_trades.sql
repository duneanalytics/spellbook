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
WITH fee_events AS (
  SELECT *
  FROM {{ Fee_evt }}
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
  SELECT
    bs.*,
    fe.fee,
    ROW_NUMBER() OVER (
      PARTITION BY bs.tx_hash, bs.evt_index
      ORDER BY fe.evt_block_time DESC
    ) AS rn
  FROM base_swaps bs
  LEFT JOIN fee_events fe
    ON fe.contract_address = bs.project_contract_address
       AND fe.evt_block_time <= bs.block_time
)
SELECT
    '{{ blockchain }}' AS blockchain,
    '{{ project }}' AS project,
    '{{ version }}' AS version,
    CAST(date_trunc('month', j.block_time) AS date) AS block_month,
    CAST(date_trunc('day', j.block_time) AS date) AS block_date,
    j.block_time,
    j.block_number,
    CAST(j.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw,
    CAST(j.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw,
    CASE WHEN j.amount0 < INT256 '0' THEN j.token0 ELSE j.token1 END AS token_bought_address,
    CASE WHEN j.amount0 < INT256 '0' THEN j.token1 ELSE j.token0 END AS token_sold_address,
    j.taker,
    j.maker,
    j.project_contract_address,
    j.tx_hash,
    j.evt_index,
    j.fee
FROM joined_fee j
WHERE j.rn = 1 OR j.rn IS NULL
{% endmacro %}