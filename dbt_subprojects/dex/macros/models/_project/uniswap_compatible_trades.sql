{% macro uniswap_compatible_v2_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PairCreated = null
    , pair_column_name = 'pair'
    )
%}
WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.to AS taker
        , t.contract_address AS maker
        , CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        , CASE WHEN amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address
        , CASE WHEN amount0In = UINT256 '0' OR amount1Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address AS project_contract_address
        , t.evt_tx_hash AS tx_hash
        , t.evt_index AS evt_index
    FROM
        {{ Pair_evt_Swap }} t
    INNER JOIN
        {{ Factory_evt_PairCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
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
FROM
    dexs
{% endmacro %}

{% macro uniswap_compatible_v3_trades(
    blockchain = null
    , project = null
    , version = null
    , Pair_evt_Swap = null
    , Factory_evt_PoolCreated = null
    , taker_column_name = 'recipient'
    , maker_column_name = null
    , optional_columns = ['f.fee']
    , pair_column_name = 'pool'
    )
%}
WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , t.{{ taker_column_name }} AS taker
        , {% if maker_column_name %}
                t.{{ maker_column_name }}
            {% else %}
                cast(null as varbinary)
            {% endif %} as maker
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        , CASE WHEN amount0 < INT256 '0' THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        , CASE WHEN amount0 < INT256 '0' THEN f.token0 ELSE f.token1 END AS token_bought_address
        , CASE WHEN amount0 < INT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address
        , t.contract_address as project_contract_address
        {% if optional_columns %}
            {% for optional_column in optional_columns %}
            , {{ optional_column }}
            {% endfor %}
        {% endif %}
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
    FROM
        {{ Pair_evt_Swap }} t
    INNER JOIN
        {{ Factory_evt_PoolCreated }} f
        ON f.{{ pair_column_name }} = t.contract_address
    {% if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
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
    , CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
FROM
    dexs
{% endmacro %}

{% macro uniswap_compatible_v4_trades(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_Swap = null
    , PoolManager_evt_Initialize = null
    , taker_column_name = null
    , maker_column_name = null
    , swap_optional_columns = ['fee']
    , initialize_optional_columns = ['hooks']
    , pair_column_name = 'id'
    )
%}
WITH dexs AS
(
    SELECT
        t.evt_block_number AS block_number
        , t.evt_block_time AS block_time
        , {% if taker_column_name -%} t.{{ taker_column_name }} {% else -%} cast(null as varbinary) {% endif -%} as taker
        , {% if maker_column_name -%} t.{{ maker_column_name }} {% else -%} cast(null as varbinary) {% endif -%} as maker      
        -- in v4, when amount is negative, then user are selling the token (so things are done from the perspective of the user instead of the pool)
        , CASE WHEN t.amount0 < INT256 '0' THEN abs(t.amount1) ELSE abs(t.amount0) END AS token_bought_amount_raw 
        , CASE WHEN t.amount0 < INT256 '0' THEN abs(t.amount0) ELSE abs(t.amount1) END AS token_sold_amount_raw
        , CASE WHEN t.amount0 < INT256 '0' THEN f.currency1 ELSE f.currency0 END AS token_bought_address
        , CASE WHEN t.amount0 < INT256 '0' THEN f.currency0 ELSE f.currency1 END AS token_sold_address
        , t.contract_address as project_contract_address
        , t.sender 
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
        {%- if swap_optional_columns %}
        {%- for optional_column in swap_optional_columns %}
        , t.{{ optional_column }}
        {%- endfor %}
        {%- endif %}
        {%- if initialize_optional_columns %}
        {%- for optional_column in initialize_optional_columns %}
        , f.{{ optional_column }}
        {%- endfor %}
        {%- endif %}
    FROM
        {{ PoolManager_evt_Swap }} t
    INNER JOIN
        {{ PoolManager_evt_Initialize }} f
        ON f.{{ pair_column_name }} = t.id
    {%- if is_incremental() %}
    WHERE
        {{ incremental_predicate('t.evt_block_time') }}
    {%- endif %}
)

SELECT
    {% if blockchain -%} '{{ blockchain }}' {% else -%} 'Unassigned' {% endif -%} as blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw
    , CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.sender as router
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
    {%- if swap_optional_columns %}  
    {%- for optional_column in swap_optional_columns %}  
    , dexs.{{ optional_column }}  
    {%- endfor %}  
    {%- endif %}  
    {%- if initialize_optional_columns %}
    {%- for optional_column in initialize_optional_columns %}
    , dexs.{{ optional_column }}
    {%- endfor %}
    {%- endif %}
FROM
    dexs
{% endmacro %}