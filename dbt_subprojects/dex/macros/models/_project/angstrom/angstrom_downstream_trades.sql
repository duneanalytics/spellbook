{% macro angstrom_downstream_trades(
    blockchain = null
    , trades_table = null
    )
%}

with 

base_trades as (
    select 
        tx_hash
        , block_time
        , block_number
        , evt_index 
        , 0 as fee -- fee columns 
    from 
    {{ trades_table }}
    where 1=1
    {%- if is_incremental() %}
    and {{ incremental_predicate('block_time') }}
    {%- endif %}  
)

, dex_trades as (
    select 
        dexs.*
        , date_trunc('minute', dexs.block_time) as block_minute -- for prices join
        , bt.fee -- fee columns 
    from 
    {{ ref('dex_trades') }} dexs 
    inner join 
    base_trades bt 
        on dexs.block_number = bt.block_number
        and dexs.tx_hash = bt.tx_hash 
        and dexs.evt_index = bt.evt_index 
    where dexs.blockchain = '{{blockchain}}'
    {%- if is_incremental() %}
    and {{ incremental_predicate('dexs.block_time') }}
    {%- endif %}
)

, prices as (
    select
        blockchain
        , contract_address
        , minute
        , price
    from
    {{ source('prices','usd_with_native') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('minute') }}
    {% endif %}
) 

    select
        dt.blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
        , dt.fee -- fee columns
        , dt.fee * p.price as fee_amount_usd -- fee columns usd assuming token_sold_address is what fee is paid in
    from 
    dex_trades dt 
    left join 
    prices p 
        on dt.token_sold_address = p.contract_address 
        and dt.block_minute = p.minute 

{% endmacro %}