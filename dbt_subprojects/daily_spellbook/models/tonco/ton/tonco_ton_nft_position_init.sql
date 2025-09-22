{{ config(
    schema = 'tonco_ton'
    , alias = 'nft_position_init'
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

-- based on reference docs: https://docs.tonco.io/technical-reference/contracts/position_nft

WITH
pools as (
    SELECT DISTINCT pool as address
    FROM {{ source('ton', 'dex_pools') }}
    WHERE project = 'tonco' -- like this https://tonviewer.com/EQD25vStEwc-h1QT1qlsYPQwqU5IiOhox5II0C_xsDNpMVo7
),
parsed_boc AS (
    SELECT M.block_date, M.tx_hash, M.trace_id, M.tx_now, M.tx_lt, M.source as pool_address, body_boc
    FROM {{ source('ton', 'messages') }} M
    WHERE
        M.block_date >= TIMESTAMP '2024-11-01' -- protocol launch
        {% if is_incremental() %}
            AND {{ incremental_predicate('M.block_date') }}
        {% endif %}
        AND M.direction = 'out'
        AND M.opcode = -705902038 -- 0xd5ecca2a
        AND M.source in (select address from POOLS)
), 
parse_output as (
    -- -705902038: 0xd5ecca2a, POSITIONNFTV3_POSITION_INIT

    SELECT 
        {{ ton_from_boc('body_boc', [
            ton_begin_parse(),
            ton_load_uint(32, 'op'),
            ton_load_uint(64, 'query_id'),
            ton_load_address('user_address'),
            ton_load_uint(128, 'liquidity'),
            ton_load_int(24, 'tickLower'),
            ton_load_int(24, 'tickUpper'),
            ton_load_ref(),
            ton_begin_parse(),
            ton_load_int(128, 'feeGrowthInside0LastX128'),
            ton_load_int(128, 'feeGrowthInside1LastX128'),
            ton_load_uint(64, 'nftIndex'),
            ton_load_coins('jetton0Amount'),
            ton_load_coins('jetton1Amount'),
            ton_load_int(24, 'tick')
        ]) }} as result, 
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
    result.user_address,
    result.liquidity,
    result.tickLower,
    result.tickUpper,
    result.feeGrowthInside0LastX128,
    result.feeGrowthInside1LastX128,
    result.nftIndex,
    result.jetton0Amount,
    result.jetton1Amount,
    result.tick
FROM parse_output