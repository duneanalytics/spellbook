{{ config(
    alias = 'trades'
    ,schema = 'odos_v2_avalanche_c'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2023-07-13' %}

with event_data as (
    select
        evt_block_time as block_time
        , evt_block_number as block_number
        , sender as taker
        , contract_address as maker
        , inputAmount as token_sold_amount_raw
        , amountOut as token_bought_amount_raw
        , cast(null as double) as amount_usd
        , case
            when inputToken = 0x0000000000000000000000000000000000000000
            then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            else inputToken
        end as token_sold_address
        , case
            when outputToken = 0x0000000000000000000000000000000000000000
            then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            else outputToken
        end as token_bought_address
        , contract_address as project_contract_address
        , evt_tx_hash as tx_hash
        , evt_index
        , cast(array[-1] as array<bigint>) as trace_address
    from {{ source('odos_v2_avalanche_c', 'OdosRouterV2_evt_Swap') }}
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% else %}
    where evt_block_time >= timestamp '{{project_start_date}}'
    {% endif %}
)

select
    'avalanche_c' as blockchain
    , 'odos' as project
    , '2' as version
    , try_cast(date_trunc('day', e.block_time) as date) as block_date
    , try_cast(date_trunc('month', e.block_time) as date) as block_month
    , e.block_time
    , erc20a.symbol as token_bought_symbol
    , erc20b.symbol as token_sold_symbol
    , case
        when lower(erc20a.symbol) > lower(erc20b.symbol)
        then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    , e.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount
    , e.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount
    , e.token_bought_amount_raw as token_bought_amount_raw
    , e.token_sold_amount_raw as token_sold_amount_raw
    , coalesce(
        e.amount_usd
        , (e.token_bought_amount_raw / power(10, erc20a.decimals)) * p_bought.price
        , (e.token_sold_amount_raw / power(10, erc20b.decimals)) * p_sold.price
    ) as amount_usd
    , cast(e.token_bought_address as varbinary) as token_bought_address
    , e.token_sold_address
    , cast(e.taker as varbinary) as taker
    , e.maker
    , e.project_contract_address
    , e.tx_hash
    , tx."from" as tx_from
    , tx.to as tx_to
    , e.evt_index
    , e.trace_address
from event_data e
inner join {{ source('avalanche_c', 'transactions') }} tx
    on e.tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= date '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('tx.block_time') }}
    {% endif %}
left join {{ source('tokens', 'erc20') }} erc20a
    on erc20a.contract_address = e.token_bought_address
    and erc20a.blockchain = 'avalanche_c'
left join {{ source('tokens', 'erc20') }} erc20b
    on erc20b.contract_address = e.token_sold_address
    and erc20b.blockchain = 'avalanche_c'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', e.block_time)
    and p_bought.contract_address = e.token_bought_address
    and p_bought.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    and p_bought.minute >= date '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('p_bought.minute') }}
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', e.block_time)
    and p_sold.contract_address = e.token_sold_address
    and p_sold.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    and p_sold.minute >= date '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('p_sold.minute') }}
    {% endif %}
