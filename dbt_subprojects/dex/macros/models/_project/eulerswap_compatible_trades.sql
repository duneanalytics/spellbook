{% macro eulerswap_compatible_trades(
    blockchain = null
    , project = null
    , version = null
    , eulerswapinstance_evt_swap = null
    , eulerswap_pools_created = null
    , univ4_PoolManager_evt_Swap = null 
    , filter = null 
    )
%}

with 

uni_v4_trades as (
    select 
        evt_tx_hash as tx_hash 
        , evt_block_time as block_time 
        , evt_index
        , evt_block_number as block_number 
    from 
    {{ univ4_PoolManager_evt_Swap }}
    where 1=1
    {%- if is_incremental() %}
    and {{ incremental_predicate('evt_block_time') }}
    {%- endif %}

),

dexs as (
    select 
        s.evt_block_time as block_time
        , s.evt_block_number as block_number 
        , ep.asset0 as token0 
        , ep.asset1 as token1
        , ep.pool as maker -- uniswap pool id 
        , ep.fee
        , ep.protocolFee
        , s.contract_address as instance -- hook
        , s.contract_address as project_contract_address -- rename for dex trades
        , ep.eulerAccount 
        , ep.factory_address
        , ep.creation_block_time as pool_creation_time 
        , s.evt_tx_hash as tx_hash
        , s.evt_index as tx_index 
    
        -- EulerSwap Instance event from pool's perspective, so flipping the sign to be user's perspective
        , case 
            when s.amount0In != 0 then -1 * cast(s.amount0In as double)
            else cast(s.amount0Out as double)
          end as amount0 
        , case 
            when s.amount1In != 0 then -1 * cast(s.amount1In as double)
            else cast(s.amount1Out as double)
          end as amount1
        , s.amount0In
        , s.amount0Out
        , s.amount1In
        , s.amount1Out
        , s.sender -- router 
        , s.evt_index
        , t.tx_hash as uni_tx_hash
    from 
    {{ eulerswapinstance_evt_swap }} s 
    left join 
    {{ eulerswap_pools_created }} ep 
        on ep.hook = s.contract_address 
    left join 
    uni_v4_trades t 
        on t.block_number = s.evt_block_number 
        and t.tx_hash = s.evt_tx_hash
        and t.evt_index = s.evt_index + 1 
    where 1 = 1 
    {% if is_incremental() %}
    and {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
)

select 
    '{{ blockchain }}' AS blockchain
    , '{{ project }}' AS project
    , '{{ version }}' AS version
    , CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    , CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    , dexs.block_time
    , dexs.block_number
    , case 
        when amount0Out != 0 then amount0Out
        else amount1Out
      end as token_bought_amount_raw 
    , case 
        when amount0In != 0 then amount0In
        else amount1In
      end as token_sold_amount_raw 
    , case 
        when amount0Out != 0 then token0
        else token1
      end as token_bought_address
    , case 
        when amount0In != 0 then token0
        else token1
      end as token_sold_address
    , dexs.sender as taker
    , dexs.project_contract_address as maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
    -- add eulerswap trades items at the end 
    , dexs.pool_creation_time
    , dexs.fee 
    , dexs.protocolFee 
    , dexs.instance 
    , dexs.eulerAccount 
    , dexs.factory_address 
    , dexs.sender 
from 
dexs 
where uni_tx_hash is null 

{% endmacro %}


{% macro eulerswap_downstream_trades(
    blockchain = null
    , project = null
    , version = null
    , eulerswapinstance_evt_swap = null
    , eulerswap_pools_created = null
    )
%}

with 

eulerswap_events as (
    select 
        s.evt_block_number as block_number 
        , ep.fee
        , ep.protocolFee
        , s.contract_address as instance -- hook
        , ep.eulerAccount 
        , ep.factory_address
        , ep.creation_block_time as pool_creation_time
        , s.evt_tx_hash as tx_hash
        , s.evt_index as tx_index
        , s.sender -- router 
    from 
    {{ eulerswapinstance_evt_swap }} s 
    left join 
    {{ eulerswap_pools_created }} ep 
        on ep.hook = s.contract_address 
    where 1 = 1 
    {% if is_incremental() %}
    and {{ incremental_predicate('s.evt_block_time') }}
    {% endif %}
),


eulerswap_og_trades as (
    select 
      dexs.*
      , ee.pool_creation_time
      , ee.fee 
      , ee.protocolFee 
      , ee.instance 
      , ee.eulerAccount 
      , ee.factory_address 
      , ee.sender 
      , 'OG' as source 
    from 
    {{ref('dex_trades')}} dexs 
    inner join 
    eulerswap_events ee 
      on dexs.block_number = ee.block_number 
      and dexs.tx_hash = ee.tx_hash 
      and dexs.evt_index = ee.tx_index 
    where dexs.blockchain = '{{blockchain}}'
    and dexs.project = '{{project}}'
    and dexs.version = '{{version}}'
    {% if is_incremental() %}
    and {{ incremental_predicate('dexs.block_time') }}
    {% endif %}
),

eulerswap_univ4_trades as (
    select 
      dexs.*
      , ee.pool_creation_time
      , ee.fee 
      , ee.protocolFee 
      , ee.instance 
      , ee.eulerAccount 
      , ee.factory_address 
      , ee.sender 
      , 'uni_v4' as source
    from 
    {{ref('dex_trades')}} dexs 
    inner join 
    eulerswap_events ee 
      on dexs.block_number = ee.block_number 
      and dexs.tx_hash = ee.tx_hash 
      and dexs.evt_index = ee.tx_index + 1 
    where dexs.blockchain = '{{blockchain}}'
    and dexs.project = 'uniswap'
    and dexs.version = '4'
    {% if is_incremental() %}
    and {{ incremental_predicate('dexs.block_time') }}
    {% endif %}
)

select * from eulerswap_og_trades

union all 

select * from eulerswap_univ4_trades

{% endmacro %}
