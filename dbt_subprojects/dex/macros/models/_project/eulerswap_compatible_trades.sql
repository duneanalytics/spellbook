{% macro eulerswap_compatible_trades(
    blockchain = null
    , project = null
    , version = null
    , eulerswapinstance_evt_swap = null
    , eulerswap_pools_created = null
    )
%}

with 

uni_v4_trades as (
    select 
        * 
    from 
    {{ ref('dex_trades') }}
    where blockchain = '{{blockchain}}'
    and project = 'uniswap'
    and version = '4'
    {% if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {% endif %}
),

latest_block_time as (
    select 
        max(block_time) as latest_block_time -- since dex.trades refreshes hourly, we need to make sure trades that happen between the last dex.trades run and eulerswap spell run aren't improperly tagged 
    from 
    uni_v4_trades
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
        , t.amount_usd -- reference / comp from dex.trades
        , s.sender -- router 
        , s.evt_index
        , case 
            when t.amount_usd is not null then 'uni_v4' 
            else 'OG' 
         end as source
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
    where s.evt_block_time <= (select latest_block_time from latest_block_time)
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
        when amount0Out != 0 then asset0
        else asset1
      end as token_bought_address
    , case 
        when amount0In != 0 then asset0
        else asset1
      end as token_sold_address
    , dexs.token_bought_address
    , dexs.token_sold_address
    , dexs.taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , dexs.evt_index
    -- add eulerswap trades items at the end 
    , dexs.fee 
    , dexs.protocolFee 
    , dexs.instance 
    , dexs.eulerAccount 
    , dexs.factory_address 
    , dexs.sender 
    , dexs.source
from 
dexs 

{% endmacro %}
