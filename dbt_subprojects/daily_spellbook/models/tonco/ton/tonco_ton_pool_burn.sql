{{ config(
       schema = 'tonco_ton'
       , alias = 'pool_burn'
       , materialized = 'incremental'
       , file_format = 'delta'
       , incremental_strategy = 'merge'
       , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
       , unique_key = ['tx_hash', 'block_date']
       , post_hook='{{ expose_spells(\'["ton"]\',
                                   "project",
                                   "tonco",
                                   \'["markfromton"]\') }}'
   )
 }}

-- based on reference docs: https://docs.tonco.io/technical-reference/contracts/pool

WITH
pools AS (
    SELECT DISTINCT pool AS address
    FROM {{ source('ton', 'dex_pools') }}
    WHERE project = 'tonco'
),
parsed_boc AS (
    SELECT
        M.block_date,
        M.tx_hash,
        M.trace_id,
        M.tx_now,
        M.tx_lt,
        M.destination AS pool_address,
        body_boc
    FROM {{ source('ton', 'messages') }} AS M
    WHERE
        M.block_date >= TIMESTAMP '2024-11-01'
        {% if is_incremental() %}
            AND {{ incremental_predicate('M.block_date') }}
        {% endif %}
        AND M.direction = 'out'
        AND M.destination IN (SELECT address FROM pools)
        AND M.opcode = -684015459 -- 0xd73ac09d
),
parse_output AS (
    -- -684015459: 0xd73ac09d, POOLV3_BURN
    SELECT
        {{ ton_from_boc('body_boc', [
            ton_begin_parse(),
            ton_load_uint(32, 'op'),
            ton_load_uint(64, 'query_id'),
            ton_load_address('recipient'),
            ton_load_uint(64, 'burned_index'),
            ton_load_uint(128, 'liquidity'),
            ton_load_int(24, 'tickLower'),
            ton_load_int(24, 'tickUpper'),
            ton_load_uint(128, 'liquidity2Burn'),
            ton_load_ref(),
            ton_begin_parse(),
            ton_load_uint(256, 'feeGrowthInside0LastX128'),
            ton_load_uint(256, 'feeGrowthInside1LastX128'),
            ton_load_ref(),
            ton_begin_parse(),
            ton_load_uint(256, 'feeGrowthInside0CurrentX128'),
            ton_load_uint(256, 'feeGrowthInside1CurrentX128')
        ]) }} AS result,
        *
    FROM parsed_boc
)
SELECT
    block_date,
    tx_hash,
    trace_id,
    tx_now,
    tx_lt,
    pool_address,
    result.op,
    result.query_id,
    result.recipient,
    result.burned_index,
    result.liquidity,
    result.tickLower,
    result.tickUpper,
    result.liquidity2Burn,
    result.feeGrowthInside0LastX128,
    result.feeGrowthInside1LastX128,
    result.feeGrowthInside0CurrentX128,
    result.feeGrowthInside1CurrentX128
FROM parse_output