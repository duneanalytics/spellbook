{% macro angstrom_downstream_trades(
    blockchain = null
    , trades_table = null
    , version = null 
    , project = null 
    )
%}

with 

base_trades as (
    select 
        tx_hash
        , block_time
        , block_number
        , evt_index 
        , 0 as token_sold_lp_fees_paid_raw -- fee columns 
        , 0 as token_bought_lp_fees_paid_raw
        , 0 as token_sold_protocol_fees_paid_raw
        , 0 as token_bought_protocol_fees_paid_raw
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
        , token_sold_lp_fees_paid_raw -- fee columns
        , token_bought_lp_fees_paid_raw
        , token_sold_protocol_fees_paid_raw
        , token_bought_protocol_fees_paid_raw
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

, tokens_metadata as (
    --erc20 tokens
    select
          blockchain
        , contract_address
        , symbol
        , decimals
    from
    {{ source ('tokens', 'erc20') }}
    where blockchain = '{{blockchain}}'
)

    select
        dt.blockchain
        , '{{project}}' as project
        , '{{version}}' as version
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
        -- fee columns
        , token_sold_lp_fees_paid_raw
        , token_bought_lp_fees_paid_raw
        , token_sold_protocol_fees_paid_raw
        , token_bought_protocol_fees_paid_raw
        , token_sold_lp_fees_paid_raw / pow(10, ta.decimals) as token_sold_lp_fees_paid
        , token_bought_lp_fees_paid_raw / pow(10, tb.decimals) as token_bought_lp_fees_paid
        , token_sold_protocol_fees_paid_raw / pow(10, ta.decimals) as token_sold_protocol_fees_paid
        , token_bought_protocol_fees_paid_raw / pow(10, tb.decimals) as token_bought_protocol_fees_paid
        , (token_sold_lp_fees_paid_raw / pow(10, ta.decimals)) * pa.price as token_sold_lp_fees_paid_usd
        , (token_bought_lp_fees_paid_raw / pow(10, tb.decimals)) * pb.price as token_bought_lp_fees_paid_usd
        , (token_sold_protocol_fees_paid_raw / pow(10, ta.decimals)) * pa.price as token_sold_protocol_fees_paid_usd
        , (token_bought_protocol_fees_paid_raw / pow(10, tb.decimals)) * pb.price as token_bought_protocol_fees_paid_usd
    from 
    dex_trades dt 
    left join 
    prices pa
        on dt.token_sold_address = pa.contract_address 
        and dt.block_minute = pa.minute 
    left join 
    prices pb
        on dt.token_bought_address = pb.contract_address 
        and dt.block_minute = pb.minute
    left join 
    tokens_metadata ta 
        on dt.token_sold_address = ta.contract_address
    left join 
    tokens_metadata tb
        on dt.token_bought_address = tb.contract_address

{% endmacro %}