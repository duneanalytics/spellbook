{% macro uniswap_compatible_v4_liquidity(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_ModifyLiquidity = null
    , PoolManager_evt_Swap = null
    , PoolManager_call_Swap = null
    , PoolManager_evt_Initialize = null
    , pair_column_name = 'id'
    )
%}

WITH filtered_modify_liquidity AS (
    SELECT 
         evt_block_time
       , evt_block_number
       , id
       , evt_tx_hash
       , evt_index
       , salt
       , tickLower
       , tickUpper
       , liquidityDelta
    FROM {{ PoolManager_evt_ModifyLiquidity }}
    {%- if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
)

, filtered_swaps_evt AS (
    SELECT 
         evt_block_time
       , evt_block_number
       , evt_tx_hash
       , evt_index
       , id
       , sqrtPriceX96
    FROM {{ PoolManager_evt_Swap }}
    {%- if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
)

, filtered_swaps_func AS (
    SELECT 
          call_block_number
        , call_block_time 
        , call_tx_hash 
        , call_trace_address
        , "key"
        , output_swapDelta
    FROM {{ PoolManager_call_Swap }}
    {%- if is_incremental() %}
    WHERE {{ incremental_predicate('call_block_time') }}
    {%- endif %}

)

, filtered_initialize AS (
    SELECT 
        evt_block_time,
        {{pair_column_name}} as id,
        currency0,
        currency1,
        sqrtPriceX96
    FROM {{ PoolManager_evt_Initialize }}
)

, swap_liquidity as (
          WITH clean_swaps AS (
                        WITH raw AS (
                            SELECT 
                              call_block_number
                            , call_block_time 
                            , call_tx_hash 
                            , call_trace_address
                            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency0')) AS currency0
                            , FROM_HEX(JSON_EXTRACT_SCALAR(JSON_PARSE("key"), '$.currency1')) AS currency1
                            , CAST(output_swapDelta AS VARBINARY) AS swapDelta_varbinary
                       FROM filtered_swaps_func  
                     )
                     , wrangled as (
                          SELECT *
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
                    , call_tx_hash
                    , currency0
                    , currency1
                    , -1 * amount0 as amount0
                    , -1 * amount1 as amount1 
                    ,  row_number() over(partition by call_tx_hash order by call_trace_address) as call_rn
                FROM wrangled
        )
        , swap_evt as (
            select 
                  evt_tx_hash
                , evt_block_time
                , evt_index
                , evt_block_number
                , id
                , sqrtPriceX96
                , row_number() over(partition by evt_tx_hash order by evt_index) as evt_rn
            FROM filtered_swaps_evt
        )

    SELECT 
        e.evt_block_number
      , e.evt_block_time
      , e.evt_tx_hash
      , e.evt_index
      , e.id
      , c.currency0 as token0
      , c.currency1 as token1
      , c.amount0
      , c.amount1
      , e.sqrtPriceX96
    FROM clean_swaps c 
    JOIN swap_evt e on c.call_block_number = e.evt_block_number 
        and c.call_tx_hash = e.evt_tx_hash
        and c.call_rn = e.evt_rn 
)
, get_recent_sqrtPriceX96 AS (
    SELECT *
    FROM (
        SELECT 
            ml.*
            ,i.currency0 as token0
            ,i.currency1 as token1
            ,COALESCE(s.evt_block_time, i.evt_block_time) as most_recent_time
            ,COALESCE(s.sqrtPriceX96, i.sqrtPriceX96) AS sqrtPriceX96
            ,ROW_NUMBER() OVER (
                PARTITION BY ml.id, ml.evt_block_time, ml.evt_index
                ORDER BY 
                    CASE WHEN s.sqrtPriceX96 IS NOT NULL THEN s.evt_block_time ELSE i.evt_block_time END DESC
            ) AS rn
        FROM filtered_modify_liquidity ml
        LEFT JOIN swap_liquidity s 
            ON ml.evt_block_time > s.evt_block_time AND ml.id = s.id
        LEFT JOIN filtered_initialize i 
            ON ml.evt_block_time >= i.evt_block_time AND i.id = ml.id
    ) tbl
    WHERE rn = 1
),

prep_for_calculations AS (
    SELECT  
          evt_block_time as block_time
        , evt_block_number as block_number
        , id 
        , evt_tx_hash as tx_hash
        , evt_index
        , salt
        , token0
        , token1
        , LOG(sqrtPriceX96/POWER(2, 96), 10)/LOG(1.0001, 10) as tickCurrent
        , tickLower
        , tickUpper
        , SQRT(POWER(1.0001, tickLower)) as sqrtRatioL
        , SQRT(POWER(1.0001, tickUpper)) sqrtRatioU
        , sqrtPriceX96/ POWER(2, 96) sqrtPrice
        , sqrtPriceX96
        , liquidityDelta
    FROM get_recent_sqrtPriceX96
),

base_liquidity_amounts AS (
    SELECT
          block_time
        , block_number
        , id 
        , tx_hash
        , evt_index
        , salt
        , token0
        , token1
        , CASE 
            WHEN sqrtPrice <= sqrtRatioL THEN 
                liquidityDelta * ((sqrtRatioU - sqrtRatioL)/(sqrtRatioL*sqrtRatioU))
            WHEN sqrtPrice >= sqrtRatioU THEN 
                0
            ELSE 
                liquidityDelta * ((sqrtRatioU - sqrtPrice)/(sqrtPrice*sqrtRatioU))
          END as amount0
        , CASE 
            WHEN sqrtPrice <= sqrtRatioL THEN 
                0
            WHEN sqrtPrice >= sqrtRatioU THEN 
                liquidityDelta*(sqrtRatioU - sqrtRatioL)
            ELSE 
                liquidityDelta*(sqrtPrice - sqrtRatioL)
          END as amount1
    FROM prep_for_calculations
)
, liquidity_change_base as (

    SELECT b.id
        , date_trunc('minute', b.block_time) as block_time
        , b.block_number
        , b.tx_hash
        , b.evt_index
        , b.token0
        , b.token1
        , b.amount0
        , b.amount1
    FROM base_liquidity_amounts b
    
    union all -- add liquidty modification together with swaps as they both impact total liquidity
    
    select s.id
        , date_trunc('minute', s.evt_block_time) as block_time
        , evt_block_number as block_number
        , s.evt_tx_hash as tx_hash
        , s.evt_index
        , token0
        , token1
        -- v4 signage is from user's perspective, so multiply -1 to flip signage to be from pool's perspective
        , -1* s.amount0
        , -1* s.amount1
    from swap_liquidity s
)
SELECT 
          '{{blockchain}}' AS blockchain
        , '{{project}}'  AS project
        , '{{version}}' AS version
        , CAST(date_trunc('month', base.block_time) AS date) AS block_month
        , CAST(date_trunc('day', base.block_time) AS date) AS block_date
        , block_time
        , block_number
        , id
        , tx_hash
        , evt_index
        , token0
        , token1
        , CAST(amount0 AS double) as amount0_raw
        , CAST(amount1 AS double) as amount1_raw
FROM liquidity_change_base base

{% endmacro %}
