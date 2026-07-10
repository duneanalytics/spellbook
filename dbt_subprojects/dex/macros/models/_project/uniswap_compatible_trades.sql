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
    , PoolManager_evt_Swap = null
    , taker_column_name = null
    , maker_column_name = null
    , filter_angstrom_addr = null
    , pool_manager_addr = '0x'
    , start_date = '2024-12-01'
    , aggregator_hooks = null
    , native_token_address = null
    )
%}
{#- native_token_address: v4 PoolKey uses address(0) for the chain's native token; on chains
    where Dune's canonical native address differs it must be remapped so the erc20-metadata,
    price and token-transfer joins downstream resolve. Known chains are covered by the override
    map below; the param is an explicit escape hatch -#}
{%- set v4_native_token_overrides = {'polygon': '0x0000000000000000000000000000000000001010'} -%}{#- POL genesis contract -#}
{%- set native_token_address = native_token_address or v4_native_token_overrides.get(blockchain) -%}
{#- aggregator_hooks: ref to the BaseAggregatorHook registry; when set, rows get an
    is_aggregator_hook_swap flag and hook swaps derive direction from the call swapDelta -#}
{%- if aggregator_hooks %}
{#- aggregator-hook swaps emit an empty Swap event (amount0=0, amount1=0), so the event-based
    direction always falls through to the ELSE branch; for those rows the direction must come
    from the call swapDelta (same swapper-perspective sign convention) -#}
{%- set buy_is_currency1 = "(ah.address IS NOT NULL AND (c.amount0 < INT256 '0' OR c.amount1 > INT256 '0')) OR (ah.address IS NULL AND (e.amount0 < INT256 '0' OR e.amount1 > INT256 '0'))" %}
{%- else %}
{%- set buy_is_currency1 = "e.amount0 < INT256 '0' OR e.amount1 > INT256 '0'" %}
{%- endif %}
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
        , contract_address
        , call_tx_hash
        , amount0
        , amount1
        {%- if native_token_address %}
        , IF(currency0 = 0x0000000000000000000000000000000000000000, {{ native_token_address }}, currency0) AS currency0
        , IF(currency1 = 0x0000000000000000000000000000000000000000, {{ native_token_address }}, currency1) AS currency1
        {%- else %}
        , currency0
        , currency1
        {%- endif %}
        , hooks
        , call_trace_address
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
    WHERE 1 = 1
        {%- if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {%- endif %}

)
    {% if aggregator_hooks %}
    , agg_hooks as (
        select address
        from {{ aggregator_hooks }}
        where blockchain = '{{ blockchain }}'
    )
    {% endif %}

    SELECT
        e.evt_block_number AS block_number
    , e.evt_block_time AS block_time
    , {% if taker_column_name -%} t.{{ taker_column_name }} {% else -%} cast(null as varbinary) {% endif -%} as taker
    , e.id as maker -- In v4, the maker (i.e. what sold the token) is the pool's virtual address. We also pass the pool ID, making it easier to join with Initialize() and retrieve hooked pool metrics.
    , CASE WHEN {{ buy_is_currency1 }} THEN ABS(c.amount1) ELSE ABS(c.amount0) END AS token_bought_amount_raw
    , CASE WHEN {{ buy_is_currency1 }} THEN ABS(c.amount0) ELSE ABS(c.amount1) END AS token_sold_amount_raw
    , CASE WHEN {{ buy_is_currency1 }} THEN c.currency1 ELSE c.currency0 END AS token_bought_address
    , CASE WHEN {{ buy_is_currency1 }} THEN c.currency0 ELSE c.currency1 END AS token_sold_address
    , e.contract_address AS project_contract_address
    , e.evt_tx_hash AS tx_hash
    , e.evt_index

    , e.sender -- router
    , c.hooks
    , e.fee
    , e.liquidity
    , e.sqrtPriceX96
    , e.tick
    , c.call_trace_address
    {%- if aggregator_hooks %}
    , (ah.address is not null) as is_aggregator_hook_swap
    {%- endif %}

    FROM clean_swaps c
    JOIN swap_evt e on c.call_block_number = e.evt_block_number
        and c.call_tx_hash = e.evt_tx_hash
        and c.call_rn = e.evt_rn
    {% if aggregator_hooks %}
    LEFT JOIN agg_hooks ah on ah.address = c.hooks
    {% endif %}
    {% if filter_angstrom_addr %}
    WHERE NOT c.hooks = {{ filter_angstrom_addr }}
    {% endif %}

)

, token_transfers as (
    SELECT 
        tx_hash
        , evt_index
        , trace_address
        , block_date
        , block_number 
        , "to" as taker 
        , contract_address as token_address 
        , amount_raw as amount 
        , case 
            when token_standard = 'erc20' then array[evt_index]
            when token_standard = 'native' then trace_address 
        end as token_index 
    FROM {{ source('tokens', 'transfers') }}
    WHERE block_date >= date '{{start_date}}'
    AND "from" = {{ pool_manager_addr }}
    AND blockchain = '{{blockchain}}'
        {%- if is_incremental() %}
        AND {{ incremental_predicate('block_time') }}
        {%- endif %}
)

, rank_token_transfers as (
    SELECT 
        * 
        , row_number() over (partition by tx_hash, token_address order by token_index) as token_rank 
    FROM 
    token_transfers
)

, rank_swap_events as (
    SELECT 
        *
        , row_number() over (partition by tx_hash, token_bought_address order by evt_index) as token_rank 
    FROM 
    dexs 
)

, get_taker as (
    SELECT 
        rse.*
        , rtt.taker as transfers_taker 
    FROM 
    rank_swap_events rse 
    left join 
    rank_token_transfers rtt 
        on rse.block_number = rtt.block_number 
        and rse.tx_hash = rtt.tx_hash 
        and rse.token_bought_address = rtt.token_address 
        and rse.token_rank = rtt.token_rank 
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
    , coalesce(dexs.taker, dexs.transfers_taker) as taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index

    , dexs.sender
    , dexs.hooks
    , dexs.fee
    , dexs.liquidity
    , dexs.sqrtPriceX96
    , dexs.tick
    , dexs.call_trace_address
    {%- if aggregator_hooks %}
    , dexs.is_aggregator_hook_swap
    {%- endif %}
FROM
    get_taker dexs
{% endmacro %}
