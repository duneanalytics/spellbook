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
    , PoolManager_call_Swap = null
    , taker_column_name = null
    , maker_column_name = null
    )
%}
WITH dexs AS
(
    WITH clean_swaps AS (
        WITH raw AS (
            SELECT 
                call_block_number
            , call_block_time 
            , call_tx_hash 
            , contract_address
            , call_trace_address
            
            -- Applying keccak256(abi.encode(poolKey)) in SQL to create the virtual pool ID
            , keccak (
                CONCAT(
                    LPAD(
                        FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency0'))
                        , 32
                        , 0x00
                    )
                    , LPAD(
                        FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency1'))
                        , 32
                        , 0x00
                    )
                    , LPAD(
                        CAST(CAST(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.fee') AS UINT256) AS VARBINARY)
                        , 32
                        , 0x00
                    )
                    , LPAD(
                        CAST(CAST(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.tickSpacing') AS INT256) AS VARBINARY)
                        , 32
                        , 0x00
                    )
                    , LPAD(
                        FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.hooks'))
                        , 32
                        , 0x00
                    )
                )
            ) AS id
            
            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency0')) AS currency0
            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency1')) AS currency1
            , CAST(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.fee') AS UINT256) AS swapFee 
            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.hooks')) AS hooks
            , CAST(JSON_EXTRACT(params, '$.zeroForOne') AS BOOLEAN) AS zeroForOne
            , JSON_EXTRACT(params, '$.amountSpecified') AS amountSpecified
            , CAST(output_swapDelta AS VARBINARY) AS swapDelta_varbinary
            
            FROM {{ PoolManager_call_Swap }}
            WHERE call_success
                {%- if is_incremental() %}
                AND {{ incremental_predicate('call_block_time') }}
                {%- endif %}
        )

        , wrangled AS (
            SELECT *
            /* Calculate amount0 and amount1 with formula; signage is from user's perspective */
            -- The top 16 bytes
            , CASE 
                WHEN BITWISE_AND(
                    VARBINARY_TO_BIGINT(VARBINARY_SUBSTRING(swapDelta_varbinary, 1, 1))
                    , FROM_BASE('80', 16) -- 0x80 as decimal 128
                ) = FROM_BASE('80', 16)
                THEN VARBINARY_TO_INT256(
                    VARBINARY_CONCAT(
                        FROM_HEX('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') -- 16 bytes of 0xFF
                        , VARBINARY_SUBSTRING(swapDelta_varbinary, 1, 16)           
                    )
                )
                ELSE VARBINARY_TO_INT256(
                    VARBINARY_CONCAT(
                        FROM_HEX('0x00000000000000000000000000000000') -- 16 bytes of 0x00
                        , VARBINARY_SUBSTRING(swapDelta_varbinary, 1, 16)
                    )
                )
            END AS amount0
            
            -- The bottom 16 bytes
            , CASE 
                WHEN BITWISE_AND(
                    VARBINARY_TO_BIGINT(VARBINARY_SUBSTRING(swapDelta_varbinary, 17, 1))
                    , FROM_BASE('80', 16)
                ) = FROM_BASE('80', 16)
                THEN VARBINARY_TO_INT256(
                    VARBINARY_CONCAT(
                        FROM_HEX('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF') -- 16 bytes of 0xFF
                        , VARBINARY_SUBSTRING(swapDelta_varbinary, 17, 16)          
                    )
                )
                ELSE VARBINARY_TO_INT256(
                    VARBINARY_CONCAT(
                        FROM_HEX('0x00000000000000000000000000000000') -- 16 bytes of 0x00
                        , VARBINARY_SUBSTRING(swapDelta_varbinary, 17, 16)
                    )
                )
            END AS amount1
            
            FROM raw
        )

        SELECT 
            call_block_number
        , call_block_time
        , id
        , contract_address
        , call_tx_hash
        , amount0
        , amount1
        , currency0
        , currency1
        , swapFee
        , hooks
        , call_trace_address

        FROM wrangled
    )

    SELECT 
        call_block_number AS block_number
    , call_block_time AS block_time
    , {% if taker_column_name -%} t.{{ taker_column_name }} {% else -%} cast(null as varbinary) {% endif -%} as taker
    , id as maker -- In v4, the maker (i.e. what sold the token) is the pool's virtual address. We also pass the pool ID, making it easier to join with Initialize() and retrieve hooked pool metrics.
    , CASE WHEN amount0 < INT256 '0' THEN ABS(amount1) ELSE ABS(amount0) END AS token_bought_amount_raw 
    , CASE WHEN amount0 < INT256 '0' THEN ABS(amount0) ELSE ABS(amount1) END AS token_sold_amount_raw
    , CASE WHEN amount0 < INT256 '0' THEN currency1 ELSE currency0 END AS token_bought_address
    , CASE WHEN amount0 < INT256 '0' THEN currency0 ELSE currency1 END AS token_sold_address
    , contract_address AS project_contract_address
    , call_tx_hash AS tx_hash
    , varbinary_to_uint256(xxhash64(to_utf8(array_join(call_trace_address, '')))) as evt_index -- we are using swap call here, so artificially creating evt_index | can't directly cast as bigint because concatenated call_trace_address can be more than 24 digits long

    , swapFee
    , hooks

    FROM clean_swaps 

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
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
    , dexs.swapFee
    , dexs.hooks
FROM
    dexs
{% endmacro %}
