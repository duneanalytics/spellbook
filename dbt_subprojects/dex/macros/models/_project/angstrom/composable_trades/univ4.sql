{% macro
    angstrom_composable_uni_v4_events(   
        angstrom_contract_addr, 
        earliest_block,
        blockchain,
        PoolManager_call_Swap = null,
        PoolManager_evt_Swap = null,
        taker_column_name = null
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
            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency0')) AS currency0
            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency1')) AS currency1
            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.hooks')) AS hooks
            , CAST(output_swapDelta AS VARBINARY) AS swapDelta_varbinary
            , CAST(JSON_EXTRACT_SCALAR(JSON_PARSE("params"), '$.amountSpecified') AS INT256) AS amount_specified 
            , CAST(JSON_EXTRACT_SCALAR(JSON_PARSE("params"), '$.zeroForOne') AS boolean) AS zero_for_one
            
            FROM {{ PoolManager_call_Swap }}
            WHERE 
                call_block_number >= {{earliest_block}} AND 
                call_success
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
        , contract_address
        , call_tx_hash
        , amount0
        , amount1
        , currency0
        , currency1
        , hooks
        , call_trace_address
        , amount_specified
        , zero_for_one
        , row_number() over(partition by call_tx_hash order by call_trace_address) as call_rn

        FROM wrangled
    )

    , swap_evt as (
    select contract_address
        , evt_tx_hash
        , evt_block_time
        , evt_index
        , row_number() over(partition by evt_tx_hash order by evt_index) as evt_rn
        , evt_block_number
        , amount0
        , amount1
        , fee
        , id
        , liquidity
        , sender -- router 
        , sqrtPriceX96
        , tick
    FROM {{ PoolManager_evt_Swap }}
    WHERE 1=1 AND evt_block_number >= {{earliest_block}}
        {%- if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {%- endif %}

)

    SELECT 
        e.evt_block_number AS block_number
    , e.evt_block_time AS block_time
    , {% if taker_column_name -%} t.{{ taker_column_name }} {% else -%} cast(null as varbinary) {% endif -%} as taker
    , e.id as maker -- In v4, the maker (i.e. what sold the token) is the pool's virtual address. We also pass the pool ID, making it easier to join with Initialize() and retrieve hooked pool metrics.
    , CASE WHEN c.amount0 < INT256 '0' THEN ABS(c.amount1) ELSE ABS(c.amount0) END AS token_bought_amount_raw 
    , CASE WHEN c.amount0 < INT256 '0' THEN ABS(c.amount0) ELSE ABS(c.amount1) END AS token_sold_amount_raw
    , CASE WHEN c.amount0 < INT256 '0' THEN c.currency1 ELSE currency0 END AS token_bought_address
    , CASE WHEN c.amount0 < INT256 '0' THEN c.currency0 ELSE currency1 END AS token_sold_address
    , e.contract_address AS project_contract_address
    , e.evt_tx_hash AS tx_hash
    , e.evt_index

    , e.sender -- router
    , c.hooks
    , c.amount0
    , c.amount1
    , e.fee
    , e.liquidity
    , e.sqrtPriceX96
    , e.tick
    , c.call_trace_address
    , c.amount_specified
    , c.zero_for_one

    FROM clean_swaps c 
    JOIN swap_evt e on c.call_block_number = e.evt_block_number 
        and c.call_tx_hash = e.evt_tx_hash
        and c.call_rn = e.evt_rn 
        AND c.hooks = {{ angstrom_contract_addr }}
        AND e.fee != 0

)

SELECT
    dexs.block_time AS block_time,
    dexs.block_number AS block_number,
    CAST(dexs.token_bought_amount_raw AS UINT256) AS token_bought_amount_raw,
    CAST(dexs.token_sold_amount_raw AS UINT256) AS token_sold_amount_raw,
    dexs.amount0 AS token_amount0,
    dexs.amount1 AS token_amount1,
    dexs.amount_specified AS amount_specified,
    dexs.token_bought_address AS token_bought_address,
    dexs.token_sold_address AS token_sold_address,
    dexs.taker AS taker,
    dexs.maker AS maker,
    dexs.project_contract_address AS project_contract_address,
    dexs.tx_hash AS tx_hash,
    dexs.evt_index AS evt_index,
    dexs.sender AS sender,
    dexs.hooks AS hooks,
    dexs.fee AS fee,
    dexs.liquidity AS liquidity,
    dexs.sqrtPriceX96 AS sqrtPriceX96,
    dexs.tick AS tick,
    dexs.zero_for_one AS zero_for_one,
    dexs.call_trace_address as call_trace_address
FROM
    dexs
{% endmacro %}
