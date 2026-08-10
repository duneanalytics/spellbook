{{ config(
    schema = 'odos_v1_avalanche_c'
    ,alias = 'trades'
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2022-11-29' %}

with dexs_raw as (
    select
        evt_block_time as block_time
        , evt_block_number as block_number
        , data_value
        , output_index
        , cast(null as varbinary) as maker
        , amountsIn[1] as token_sold_amount_raw
        , amountsOut[output_index] as token_bought_amount_raw
        , cast(null as double) as amount_usd
        , case
            when tokensIn[1] = 0x0000000000000000000000000000000000000000
            then 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- WAVAX
            else tokensIn[1]
        end as token_sold_address
        , contract_address as project_contract_address
        , evt_tx_hash as tx_hash
        , evt_index
        , cast(array[-1, output_index] as array<bigint>) as trace_address
    from {{ source('odos_avalanche_c', 'OdosRouter_evt_Swapped') }}
    cross join unnest(outputs) with ordinality as u(data_value, output_index)
    {% if is_incremental() %}
    where {{ incremental_predicate('evt_block_time') }}
    {% else %}
    where evt_block_time >= timestamp '{{project_start_date}}'
    {% endif %}
)

, dexs as (
    select
        *
        , json_extract_scalar(data_value, '$.receiver') as taker
        , case
            when json_extract_scalar(data_value, '$.tokenAddress') in ('0', 'O', '0x0000000000000000000000000000000000000000')
            then '0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7' -- WAVAX
            else json_extract_scalar(data_value, '$.tokenAddress')
        end as token_bought_address
    from dexs_raw
)

select
    'avalanche_c' as blockchain
    , 'odos' as project
    , '1' as version
    , try_cast(date_trunc('day', dexs.block_time) as date) as block_date
    , try_cast(date_trunc('month', dexs.block_time) as date) as block_month
    , dexs.block_time
    , erc20a.symbol as token_bought_symbol
    , erc20b.symbol as token_sold_symbol
    , case
        when lower(erc20a.symbol) > lower(erc20b.symbol)
        then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    , dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount
    , dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount
    , dexs.token_bought_amount_raw as token_bought_amount_raw
    , dexs.token_sold_amount_raw as token_sold_amount_raw
    , coalesce(
        dexs.amount_usd
        , (dexs.token_bought_amount_raw / power(10, erc20a.decimals)) * p_bought.price
        , (dexs.token_sold_amount_raw / power(10, erc20b.decimals)) * p_sold.price
    ) as amount_usd
    , from_hex(substr(dexs.token_bought_address, 3)) as token_bought_address
    , dexs.token_sold_address
    , coalesce(cast(dexs.taker as varbinary), tx."from") as taker
    , dexs.maker
    , dexs.project_contract_address
    , dexs.tx_hash
    , tx."from" as tx_from
    , tx.to as tx_to
    , dexs.evt_index
    , dexs.trace_address
from dexs
inner join {{ source('avalanche_c', 'transactions') }} tx
    on tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    and tx.block_time >= date '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('tx.block_time') }}
    {% endif %}
left join {{ source('tokens', 'erc20') }} erc20a
    on cast(erc20a.contract_address as varchar) = dexs.token_bought_address
    and erc20a.blockchain = 'avalanche_c'
left join {{ source('tokens', 'erc20') }} erc20b
    on erc20b.contract_address = dexs.token_sold_address
    and erc20b.blockchain = 'avalanche_c'
left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and cast(p_bought.contract_address as varchar) = dexs.token_bought_address
    and p_bought.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    and p_bought.minute >= date '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('p_bought.minute') }}
    {% endif %}
left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    and p_sold.minute >= date '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('p_sold.minute') }}
    {% endif %}
