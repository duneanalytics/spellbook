{% macro robinhood_raw_v2_compatible_trades(
    project,
    version,
    factory_address,
    factory_topic,
    pool_data_word,
    start_date
) %}

{% set swap_topic = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' %}

with pair_created as (
    select distinct
        varbinary_substring(topic1, 13, 20) as token0,
        varbinary_substring(topic2, 13, 20) as token1,
        varbinary_substring(data, {{ ((pool_data_word - 1) * 32) + 13 }}, 20) as pair
    from {{ source('robinhood', 'logs') }}
    where contract_address = {{ factory_address }}
        and topic0 = {{ factory_topic }}
        and block_date >= date '{{ start_date }}'
),

swaps as (
    select
        l.block_number,
        l.block_time,
        l.contract_address,
        l.tx_hash,
        l.index as evt_index,
        varbinary_substring(l.topic2, 13, 20) as taker,
        varbinary_to_uint256(varbinary_substring(l.data, 1, 32)) as amount0_in,
        varbinary_to_uint256(varbinary_substring(l.data, 33, 32)) as amount1_in,
        varbinary_to_uint256(varbinary_substring(l.data, 65, 32)) as amount0_out,
        varbinary_to_uint256(varbinary_substring(l.data, 97, 32)) as amount1_out
    from {{ source('robinhood', 'logs') }} l
    inner join pair_created f on f.pair = l.contract_address
    where l.topic0 = {{ swap_topic }}
        and l.block_date >= date '{{ start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('l.block_time') }}
        {% endif %}
)

select
    'robinhood' as blockchain,
    '{{ project }}' as project,
    '{{ version }}' as version,
    cast(date_trunc('month', s.block_time) as date) as block_month,
    cast(date_trunc('day', s.block_time) as date) as block_date,
    s.block_time,
    s.block_number,
    case when s.amount0_out = uint256 '0' then s.amount1_out else s.amount0_out end as token_bought_amount_raw,
    case when s.amount0_out = uint256 '0' then s.amount0_in else s.amount1_in end as token_sold_amount_raw,
    case when s.amount0_out = uint256 '0' then f.token1 else f.token0 end as token_bought_address,
    case when s.amount0_out = uint256 '0' then f.token0 else f.token1 end as token_sold_address,
    s.taker,
    s.contract_address as maker,
    s.contract_address as project_contract_address,
    s.tx_hash,
    s.evt_index
from swaps s
inner join pair_created f on f.pair = s.contract_address

{% endmacro %}


{% macro robinhood_raw_v3_compatible_trades(
    project,
    version,
    factory_address,
    factory_topic,
    pool_data_word,
    swap_topic,
    start_date
) %}

with pool_created as (
    select distinct
        varbinary_substring(topic1, 13, 20) as token0,
        varbinary_substring(topic2, 13, 20) as token1,
        varbinary_substring(data, {{ ((pool_data_word - 1) * 32) + 13 }}, 20) as pool
    from {{ source('robinhood', 'logs') }}
    where contract_address = {{ factory_address }}
        and topic0 = {{ factory_topic }}
        and block_date >= date '{{ start_date }}'
),

swaps as (
    select
        l.block_number,
        l.block_time,
        l.contract_address,
        l.tx_hash,
        l.index as evt_index,
        varbinary_substring(l.topic2, 13, 20) as taker,
        varbinary_to_int256(varbinary_substring(l.data, 1, 32)) as amount0,
        varbinary_to_int256(varbinary_substring(l.data, 33, 32)) as amount1
    from {{ source('robinhood', 'logs') }} l
    inner join pool_created f on f.pool = l.contract_address
    where l.topic0 = {{ swap_topic }}
        and l.block_date >= date '{{ start_date }}'
        {% if is_incremental() %}
        and {{ incremental_predicate('l.block_time') }}
        {% endif %}
)

select
    'robinhood' as blockchain,
    '{{ project }}' as project,
    '{{ version }}' as version,
    cast(date_trunc('month', s.block_time) as date) as block_month,
    cast(date_trunc('day', s.block_time) as date) as block_date,
    s.block_time,
    s.block_number,
    cast(case when s.amount0 < int256 '0' then abs(s.amount0) else abs(s.amount1) end as uint256) as token_bought_amount_raw,
    cast(case when s.amount0 < int256 '0' then abs(s.amount1) else abs(s.amount0) end as uint256) as token_sold_amount_raw,
    case when s.amount0 < int256 '0' then f.token0 else f.token1 end as token_bought_address,
    case when s.amount0 < int256 '0' then f.token1 else f.token0 end as token_sold_address,
    s.taker,
    cast(null as varbinary) as maker,
    s.contract_address as project_contract_address,
    s.tx_hash,
    s.evt_index
from swaps s
inner join pool_created f on f.pool = s.contract_address

{% endmacro %}
