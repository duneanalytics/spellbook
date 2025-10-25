{% macro uniswap_compatible_v4_liquidity_sqrtpricex96(
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_Initialize = null
    , PoolManager_evt_Swap = null 
    )
%}

{% if is_incremental() %}

-- force price reload

with

base_events as (
    select 
        id
        , 'include' as check_filter
        , evt_block_time as block_time
        , evt_block_number as block_number
        , evt_index
        , evt_block_number + evt_index/1e6 as block_index_sum
        , sqrtpricex96
    from 
    {{ PoolManager_evt_Initialize }}
    where sqrtPriceX96 is not null 
    and {{ incremental_predicate('evt_block_time') }}

    union all 

    select 
        id
        , 'include' as check_filter
        , evt_block_time as block_time
        , evt_block_number as block_number
        , evt_index
        , evt_block_number + evt_index/1e6 as block_index_sum 
        , sqrtpricex96
    from 
    {{ PoolManager_evt_Swap }}
    where sqrtPriceX96 is not null 
    and {{ incremental_predicate('evt_block_time') }}
),

get_active_pools as ( -- get only the pools that were active on incremental run
    select 
       distinct id
    from 
    base_events 
),

get_latest_active_pools as (
    select 
        th.id
        , 'exclude' as check_filter
        , max_by(th.block_time, th.block_index_sum) as block_time
        , max_by(th.block_number, th.block_index_sum) as block_number
        , max_by(th.evt_index, th.block_index_sum) as evt_index
        , max(block_index_sum) as block_index_sum 
        , max_by(sqrtpricex96, th.block_index_sum) as sqrtpricex96
    from (
        select 
            th.*,
            be.block_index_sum as base_block_index_sum
        from 
        {{this}} th 
        inner join 
        get_active_pools ga 
            on th.id = ga.id 
        left join 
        base_events be 
            on th.id = be.id 
            and th.block_index_sum = be.block_index_sum 
    ) th 
    where base_block_index_sum is null 
    group by 1

    union all 

    select 
        id
        , check_filter
        , block_time
        , block_number
        , evt_index
        , block_index_sum 
        , sqrtpricex96
    from 
    base_events
), 

sort_table as (
    select 
        *
        , lag(block_index_sum, 1, 0) over (partition by id order by block_index_sum) as previous_block_index_sum
    from 
    get_latest_active_pools
)

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , id
        , block_time
        , block_number
        , evt_index 
        , block_index_sum 
        , previous_block_index_sum
        , sqrtpricex96
    from 
    sort_table 
    where check_filter = 'include'


{% else %}



    with 

    get_events as (
        select 
            id
            , evt_block_time as block_time
            , evt_block_number as block_number
            , evt_index 
            , evt_block_number + evt_index/1e6 as block_index_sum 
            , sqrtpricex96
        from 
        {{ PoolManager_evt_Initialize }}
        where sqrtPriceX96 is not null 

        union all 

        select 
            id
            , evt_block_time as block_time
            , evt_block_number as block_number
            , evt_index 
            , evt_block_number + evt_index/1e6 as block_index_sum
            , sqrtpricex96 
        from 
        {{ PoolManager_evt_Swap }}
        where sqrtPriceX96 is not null 
    )
    

    select 
        '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , id
        , block_time
        , block_number
        , evt_index 
        , block_index_sum 
        , lag(block_index_sum, 1, 0) over (partition by id order by block_index_sum) as previous_block_index_sum
        , sqrtpricex96
    from 
    get_events 

{% endif %}

{% endmacro %}

{% macro uniswap_compatible_v4_base_liquidity_events( 
    blockchain = null
    , project = 'uniswap'
    , version = '4'
    , PoolManager_evt_ModifyLiquidity = null
    , PoolManager_evt_Swap = null
    , PoolManager_call_ModifyLiquidity = null
    , liquidity_pools = null
    , liquidity_sqrtpricex96 = null
    )
%}


with 

get_pools as (
    select 
        blockchain
        , id
        , token0
        , token1
    from 
    {{ liquidity_pools }}
),

get_prices_tmp as (
    select
        blockchain
        , id
        , block_index_sum 
        , previous_block_index_sum
        , sqrtpricex96
    from 
    {{ liquidity_sqrtpricex96 }}
),

get_latest_prices as (
    select 
        blockchain
        , id 
        , max(block_index_sum) as block_index_sum 
        , max(previous_block_index_sum) as previous_block_index_sum 
        , max_by(sqrtpricex96, block_index_sum) as sqrtpricex96
    from 
    get_prices_tmp 
    group by 1, 2 
),

get_prices as (
    select 
        blockchain
        , id
        , block_index_sum 
        , previous_block_index_sum
        , sqrtpricex96
    from 
    get_prices_tmp 

    union all 

    select 
        blockchain
        , id
        , block_index_sum + block_index_sum as block_index_sum 
        , block_index_sum as previous_block_index_sum -- for filling
        , sqrtpricex96
    from 
    get_latest_prices
),

get_events as (
    select 
        *,
        evt_block_number + evt_index/1e6 as block_number_index 

    from 
    {{ PoolManager_evt_ModifyLiquidity }}
    {%- if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
),

enrich_liquidity_events as (
    select 
        ab.*,
        gp.sqrtpricex96
    from (
    select 
        ge.*
        , gp.previous_block_index_sum
    from 
    get_events ge 
    left join 
    get_prices gp 
        on ge.id = gp.id 
        and ge.block_index_sum >= gp.previous_block_index_sum
        and ge.block_index_sum < gp.block_index_sum 
    ) ab 
    left join 
    get_prices gp 
        on ab.id = gp.id
        and ab.previous_block_index_sum = gp.block_index_sum 
),

modify_liquidity_events as (
    with 

    get_calls as (
        select 
            contract_address,
            call_success,
            call_tx_hash,
            call_tx_from,
            call_tx_to,
            call_trace_address,
            call_block_time,
            call_block_number,
            -- pool key: currency0/1 + hooks (all varbinary)
            FROM_HEX(JSON_EXTRACT_SCALAR(key_json, '$.currency0')) AS currency0,
            FROM_HEX(JSON_EXTRACT_SCALAR(key_json, '$.currency1')) AS currency1,
            FROM_HEX(JSON_EXTRACT_SCALAR(key_json, '$.hooks'))     AS hooks,

            -- raw packed outputs (two signed 128-bit legs inside an int256)
            CAST(output_callerDelta   AS VARBINARY) AS callerDelta_vb,
            CAST(output_feesAccrued   AS VARBINARY) AS feesAccrued_vb,

            -- params (decoded for handy metadata)
            CAST(JSON_EXTRACT(params_json, '$.tickLower')      AS BIGINT)  AS tickLower,
            CAST(JSON_EXTRACT(params_json, '$.tickUpper')      AS BIGINT)  AS tickUpper,
            CAST(CAST(JSON_EXTRACT(params_json, '$.liquidityDelta') AS VARCHAR) AS INT256) AS params_liquidityDelta,

            -- for deterministic call↔event pairing within a tx
            ROW_NUMBER() OVER (PARTITION BY call_tx_hash ORDER BY call_trace_address) AS call_rn
        from (
            select 
                *, 
                json_parse("key") as key_json,
                json_parse(params) as params_json
            from 
            {{ PoolManager_call_ModifyLiquidity }}
            where call_success 
            {%- if is_incremental() %}
            and {{ incremental_predicate('call_block_time') }}
            {%- endif %} 
        ) 
    ),

    calls_decoded as (
        SELECT
            c.*,

            -- ---- decode callerDelta (top/bottom 16 bytes, sign-extended to int256) ----
            CASE
                WHEN BITWISE_AND(VARBINARY_TO_BIGINT(VARBINARY_SUBSTRING(c.callerDelta_vb, 1, 1)), FROM_BASE('80', 16)) = FROM_BASE('80', 16)
                THEN VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'), VARBINARY_SUBSTRING(c.callerDelta_vb, 1, 16)))
                ELSE VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0x00000000000000000000000000000000'), VARBINARY_SUBSTRING(c.callerDelta_vb, 1, 16)))
            END AS callerDelta_token0,

            CASE
                WHEN BITWISE_AND(VARBINARY_TO_BIGINT(VARBINARY_SUBSTRING(c.callerDelta_vb, 17, 1)), FROM_BASE('80', 16)) = FROM_BASE('80', 16)
                THEN VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'), VARBINARY_SUBSTRING(c.callerDelta_vb, 17, 16)))
                ELSE VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0x00000000000000000000000000000000'), VARBINARY_SUBSTRING(c.callerDelta_vb, 17, 16)))
            END AS callerDelta_token1,

            -- ---- decode feesAccrued (top/bottom 16 bytes, sign-extended to int256) ----
            CASE
                WHEN BITWISE_AND(VARBINARY_TO_BIGINT(VARBINARY_SUBSTRING(c.feesAccrued_vb, 1, 1)), FROM_BASE('80', 16)) = FROM_BASE('80', 16)
                THEN VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'), VARBINARY_SUBSTRING(c.feesAccrued_vb, 1, 16)))
                ELSE VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0x00000000000000000000000000000000'), VARBINARY_SUBSTRING(c.feesAccrued_vb, 1, 16)))
            END AS feesAccrued_token0,

            CASE
                WHEN BITWISE_AND(VARBINARY_TO_BIGINT(VARBINARY_SUBSTRING(c.feesAccrued_vb, 17, 1)), FROM_BASE('80', 16)) = FROM_BASE('80', 16)
                THEN VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'), VARBINARY_SUBSTRING(c.feesAccrued_vb, 17, 16)))
                ELSE VARBINARY_TO_INT256(VARBINARY_CONCAT(FROM_HEX('0x00000000000000000000000000000000'), VARBINARY_SUBSTRING(c.feesAccrued_vb, 17, 16)))
            END AS feesAccrued_token1
        FROM 
        get_calls c
    ),


    evts as (
        select
            contract_address,
            evt_tx_hash,
            evt_tx_from,
            evt_block_time,
            evt_block_number,
            evt_index,
            id,                -- pool id lives here
            sender,            -- caller/sender (useful metadata)
            tickLower,
            tickUpper,
            liquidityDelta,    -- int256 in the event log
            salt,
            sqrtpricex96,
            ROW_NUMBER() OVER (PARTITION BY evt_tx_hash ORDER BY evt_index) AS evt_rn
        from 
        enrich_liquidity_events 
    )

        SELECT
            e.id 
            , cd.call_tx_from as tx_from 
            , e.evt_block_time as block_time 
            , e.evt_block_number as block_number 
            , e.evt_tx_hash as tx_hash
            , e.evt_index
            , e.evt_block_number + e.evt_index/1e6 as block_index_sum
            , 'modify_liquidity' as event_type 
            , cd.currency0 as token0 
            , cd.currency1 as token1
            , e.tickLower 
            , e.tickUpper   
            , e.liquidityDelta
            , e.salt 
            , e.sqrtpricex96
            -- decoded outputs (signed int256, raw token units)
            -- output_callerDelta signage is from the POV of user, so we must flip signs for pool's POV
            , -1* cd.callerDelta_token0 as amount0
            , -1* cd.callerDelta_token1 as amount1
            , -1* cd.feesAccrued_token0  as fee_amount0
            , -1* cd.feesAccrued_token1  as fee_amount1
        FROM 
        evts e
        INNER JOIN 
        calls_decoded cd
            ON cd.call_tx_hash = e.evt_tx_hash
            AND cd.call_rn = e.evt_rn
), 

get_swap_events as (
    select 
        * 
    from 
    {{ PoolManager_evt_Swap }}
    {%- if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {%- endif %}
),

swap_events as (
    select 
        evt_tx_from as tx_from
        , evt_block_time
        , evt_block_number 
        , evt_tx_hash 
        , evt_index 
        , id 
        , -1 * amount0 as amount0
        , -1 * amount1 as amount1
        , cast(liquidity as int256) as liquidityDelta
        , sqrtPriceX96
        , cast(null as double) as tickLower
        , cast(null as double) as tickUpper
        , cast(null as varbinary) as salt 
    from 
    get_swap_events
),

swap_fees_paid as (
    select 
        evt_tx_from as tx_from
        , evt_block_time
        , evt_block_number 
        , evt_tx_hash 
        , evt_index 
        , id 
        , if (amount0 < int256 '0', abs(amount0) * fee/1e6, 0) as amount0
        , if (amount1 < int256 '0', abs(amount1) * fee/1e6, 0) as amount1
        , cast(liquidity as int256) as liquidityDelta
        , sqrtPriceX96
        , cast(null as double) as tickLower
        , cast(null as double) as tickUpper
        , cast(null as varbinary) as salt 
    from 
    get_swap_events
),

liquidity_change_base as (
    select 
        ml.id
        , ml.tx_from 
        , ml.block_time
        , ml.block_number 
        , ml.tx_hash 
        , ml.evt_index 
        , ml.event_type 
        , ml.token0 
        , ml.token1 
        , ml.amount0 
        , ml.amount1 
        , ml.liquidityDelta
        , ml.sqrtPriceX96
        , ml.tickLower
        , ml.tickUpper
        , ml.salt
    from 
    modify_liquidity_events ml 

    union all 

    select 
        ml.id
        , ml.tx_from 
        , ml.block_time
        , ml.block_number 
        , ml.tx_hash 
        , ml.evt_index 
        , 'fees_accrued' as event_type
        , ml.token0 
        , ml.token1 
        , ml.fee_amount0 
        , ml.fee_amount1 
        , ml.liquidityDelta
        , ml.sqrtPriceX96
        , ml.tickLower
        , ml.tickUpper
        , ml.salt
    from 
    modify_liquidity_events ml 

    union all 

    select 
        se.id
        , se.tx_from 
        , se.evt_block_time as block_time
        , se.evt_block_number as block_number 
        , se.evt_tx_hash as tx_hash 
        , se.evt_index 
        , 'swap' as event_type 
        , gp.token0 
        , gp.token1 
        , se.amount0 
        , se.amount1
        , se.liquidityDelta
        , se.sqrtPriceX96
        , se.tickLower
        , se.tickUpper
        , se.salt
    from 
    swap_events se
    inner join 
    get_pools gp 
        on se.id = gp.id 

    union all 

    select 
        se.id
        , se.tx_from 
        , se.evt_block_time as block_time
        , se.evt_block_number as block_number 
        , se.evt_tx_hash as tx_hash 
        , se.evt_index 
        , 'swap_fees_paid' as event_type 
        , gp.token0 
        , gp.token1 
        , se.amount0 
        , se.amount1 
        , se.liquidityDelta
        , se.sqrtPriceX96
        , se.tickLower
        , se.tickUpper
        , se.salt
    from 
    swap_fees_paid se
    inner join 
    get_pools gp 
        on se.id = gp.id 
)
    select 
          '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , cast(date_trunc('month', block_time) as date) as block_month
        , cast(date_trunc('day', block_time) as date) as block_date
        , date_trunc('minute', block_time) as block_time -- for prices
        , block_number
        , id
        , tx_hash
        , tx_from
        , evt_index
        , event_type
        , token0
        , token1
        , CAST(amount0 AS double) as amount0_raw
        , CAST(amount1 AS double) as amount1_raw
        , liquidityDelta
        , sqrtPriceX96
        , tickLower
        , tickUpper
        , salt
    from 
    liquidity_change_base 

    -- push pr 

{% endmacro %}

{% macro uniswap_compatible_v3_base_liquidity_events( 
    blockchain = null
    , project = 'uniswap'
    , version = null 
    , token_transfers = null 
    , liquidity_pools = null
    )
%}

with 

get_pools as (
    select 
        blockchain
        , id
        , token0
        , token1
    from 
    {{ liquidity_pools }}
    where version = '{{version}}'
),

token_tfers as (
    -- token0 out tfers
    select 
        gp.id
        , tt.evt_tx_from as tx_from 
        , tt.evt_block_time as block_time 
        , tt.evt_block_number as block_number 
        , tt.evt_tx_hash as tx_hash 
        , tt.evt_index 
        , 'token0_out' as event_type 
        , gp.token0 
        , gp.token1 
        , -1 * cast(tt.value as double) as amount0 
        , 0 as amount1 
    from 
    {{ token_transfers }} tt 
    inner join 
    get_pools gp 
        on gp.id = tt."from"
        and gp.token0 = tt.contract_address 
    {%- if is_incremental() %}
    where {{ incremental_predicate('tt.evt_block_time') }}
    {%- endif %}

    union all 

    -- token0 in tfers
    select 
        gp.id
        , tt.evt_tx_from as tx_from 
        , tt.evt_block_time as block_time 
        , tt.evt_block_number as block_number 
        , tt.evt_tx_hash as tx_hash 
        , tt.evt_index 
        , 'token0_in' as event_type 
        , gp.token0 
        , gp.token1 
        , cast(tt.value as double) as amount0 
        , 0 as amount1 
    from 
    {{ token_transfers }} tt 
    inner join 
    get_pools gp 
        on gp.id = tt.to 
        and gp.token0 = tt.contract_address 
    {%- if is_incremental() %}
    where {{ incremental_predicate('tt.evt_block_time') }}
    {%- endif %}

    union all 

    -- token1 out tfers
    select 
        gp.id
        , tt.evt_tx_from as tx_from 
        , tt.evt_block_time as block_time 
        , tt.evt_block_number as block_number 
        , tt.evt_tx_hash as tx_hash 
        , tt.evt_index 
        , 'token1_out' as event_type 
        , gp.token0 
        , gp.token1 
        , 0 as amount0
        , -1 * cast(tt.value as double) as amount1
    from 
    {{ token_transfers }} tt 
    inner join 
    get_pools gp 
        on gp.id = tt."from"
        and gp.token1 = tt.contract_address 
    {%- if is_incremental() %}
    where {{ incremental_predicate('tt.evt_block_time') }}
    {%- endif %}

    union all 

    -- token1 in 
    select 
        gp.id
        , tt.evt_tx_from as tx_from 
        , tt.evt_block_time as block_time 
        , tt.evt_block_number as block_number 
        , tt.evt_tx_hash as tx_hash 
        , tt.evt_index 
        , 'token1_in' as event_type 
        , gp.token0 
        , gp.token1 
        , 0 as amount0
        , cast(tt.value as double) as amount1
    from 
    {{ token_transfers }} tt 
    inner join 
    get_pools gp 
        on gp.id = tt.to 
        and gp.token1 = tt.contract_address 
    {%- if is_incremental() %}
    where {{ incremental_predicate('tt.evt_block_time') }}
    {%- endif %}
)

    select 
          '{{blockchain}}' as blockchain
        , '{{project}}'  as project
        , '{{version}}' as version
        , cast(date_trunc('month', block_time) as date) as block_month
        , cast(date_trunc('day', block_time) as date) as block_date
        , date_trunc('minute', block_time) as block_time -- for prices
        , block_number
        , id
        , tx_hash
        , tx_from
        , evt_index
        , event_type
        , token0
        , token1
        , amount0 as amount0_raw
        , amount1 as amount1_raw
    from 
    token_tfers 
{% endmacro %}