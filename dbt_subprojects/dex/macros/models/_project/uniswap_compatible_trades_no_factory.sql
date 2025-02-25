{% macro uniswap_compatible_v2_trades_no_factory(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    )
%}
WITH transfers AS (
    SELECT 
        tx_hash,
        contract_address as token,
        MIN_BY("from", evt_index) as token_from,
        MIN_BY("to", evt_index) as token_to,
        MIN(evt_index) as first_evt_index,
        MAX(evt_index) as last_evt_index
    FROM {{ source('tokens', 'transfers') }}
    WHERE blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_time') }}
    {% endif %}
    GROUP BY tx_hash, contract_address
),
dexs AS (
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        , CASE 
            WHEN amount0Out = UINT256 '0' THEN tf_out.token  -- token1 was bought (transferred out)
            ELSE tf_in.token                                 -- token0 was bought (transferred in)
          END AS token_bought_address
        , CASE 
            WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN tf_out.token  -- token1 was sold (transferred out)
            ELSE tf_in.token                                                            -- token0 was sold (transferred in)
          END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM {{ Pair_evt_Swap }} t
    -- Join with transfers to get the tokens involved
    INNER JOIN transfers tf_in
        ON tf_in.tx_hash = t.evt_tx_hash 
        AND tf_in.token_to = t.to  -- Token transferred to the recipient
    INNER JOIN transfers tf_out
        ON tf_out.tx_hash = t.evt_tx_hash
        AND tf_out.token_from = t.contract_address  -- Token transferred out of the pair contract
        AND tf_out.token != tf_in.token  -- Must be different tokens
    WHERE tf_in.first_evt_index < t.evt_index  -- Transfer events should happen before swap
        AND tf_out.last_evt_index > t.evt_index  -- Transfer events should happen after swap
    {% if is_incremental() %}
    AND {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , dexs.token_bought_amount_raw
    , dexs.token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM dexs

{% endmacro %}
