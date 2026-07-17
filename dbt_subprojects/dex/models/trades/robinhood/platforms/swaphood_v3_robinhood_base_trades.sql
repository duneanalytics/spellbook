{{
    config(
        schema = 'swaphood_v3_robinhood',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set swap_topic = '0x19b47279256b2a23a1665c810c8d55a1758940ee09377d4f8d26497a3577dc83' %}

with pool_created as (
    select
        pool,
        token0,
        token1
    from {{ source('swaphood_robinhood', 'PancakeV3Factory_evt_PoolCreated') }}
    where contract_address = 0x0ec554f0bff0be6c99d1e95c8015bb0950f6a2c7
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
        and l.block_date >= date '2026-07-11'
        {% if is_incremental() %}
        and {{ incremental_predicate('l.block_time') }}
        {% endif %}
)

select
    'robinhood' as blockchain,
    'swaphood' as project,
    '3' as version,
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
