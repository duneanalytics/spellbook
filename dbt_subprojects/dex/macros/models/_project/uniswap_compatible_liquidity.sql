{% macro uniswap_compatible_v4_liquidity(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_ModifyLiquidity = null
    , PoolManager_evt_Swap = null
    , PoolManager_evt_Initialize = null
    , pair_column_name = 'id'
    )
%}
WITH get_recent_sqrtPriceX96 AS
(
    SELECT *
      FROM (
                SELECT 
                        ml.*
                        ,i.currency0 as token0
                        , i.currency1 as token1
                        , COALESCE(s.evt_block_time, i.evt_block_time) as most_recent_time
                        , COALESCE(s.sqrtPriceX96, i.sqrtPriceX96) AS sqrtPriceX96
                        , ROW_NUMBER() OVER (
                            PARTITION BY ml.id, ml.evt_block_time
                            ORDER BY 
                                CASE WHEN s.sqrtPriceX96 IS NOT NULL THEN s.evt_block_time ELSE i.evt_block_time END DESC
                        ) AS rn
                    FROM 
                        {{ PoolManager_evt_ModifyLiquidity }} ml
                    LEFT JOIN 
                        {{ PoolManager_evt_Swap }} s ON ml.evt_block_time > s.evt_block_time AND ml.id = s.id
                    LEFT JOIN 
                        {{ PoolManager_evt_Initialize }} i ON ml.evt_block_time >= i.evt_block_time AND i.{{pair_column_name}} = ml.id
                    {%- if is_incremental() %}
                        WHERE
                            {{ incremental_predicate('ml.evt_block_time') }}
                    {%- endif %}
            )tbl
        WHERE rn = 1
),
prep_for_calculations AS (
    SELECT  evt_block_time as block_time
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
base_amounts AS (
    SELECT
          block_time
        , block_number
        , id 
        , tx_hash
        , evt_index
        , salt
        , token0
        , token1
        , CASE WHEN sqrtPrice <= sqrtRatioL THEN liquidityDelta * ((sqrtRatioU - sqrtRatioL)/(sqrtRatioL*sqrtRatioU))
               WHEN sqrtPrice >= sqrtRatioU THEN 0
               ELSE liquidityDelta * ((sqrtRatioU - sqrtPrice)/(sqrtPrice*sqrtRatioU))
           END as amount0
        , CASE WHEN sqrtPrice <= sqrtRatioL THEN 0
               WHEN sqrtPrice >= sqrtRatioU THEN liquidityDelta*(sqrtRatioU - sqrtRatioL)
               ELSE liquidityDelta*(sqrtPrice - sqrtRatioL)
           END as amount1
    FROM prep_for_calculations pc
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
        , salt
        , token0
        , token1
        , CAST(amount0 AS double) as amount0_raw
        , CAST(amount1 AS double) as amount1_raw
  FROM  base_amounts base

{% endmacro %}
