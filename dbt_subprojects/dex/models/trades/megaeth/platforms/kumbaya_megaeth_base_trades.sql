{{
    config(
        schema = 'kumbaya_megaeth',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with

dexs as (
    select
        t.evt_block_number as block_number,
        t.evt_block_time as block_time,
        t.recipient as taker,
        cast(null as varbinary) as maker,
        case when amount0 < int256 '0' then abs(amount0) else abs(amount1) end as token_bought_amount_raw,
        case when amount0 < int256 '0' then abs(amount1) else abs(amount0) end as token_sold_amount_raw,
        case when amount0 < int256 '0' then f.token0 else f.token1 end as token_bought_address,
        case when amount0 < int256 '0' then f.token1 else f.token0 end as token_sold_address,
        t.contract_address as project_contract_address,
        f.fee,
        t.evt_tx_hash as tx_hash,
        t.evt_index
    from {{ source('kumbaya_megaeth', 'v3pool_evt_swap') }} as t
    inner join {{ source('kumbaya_megaeth', 'v3factory_evt_poolcreated') }} as f on f.pool = t.contract_address
    where t.evt_block_time >= timestamp '2026-01-30' -- exclude stress test trades (3B+ rows)
        {% if is_incremental() %}
        and {{ incremental_predicate('t.evt_block_time') }}
        {% endif %}
)

select
    'megaeth' as blockchain,
    'kumbaya' as project,
    '1' as version,
    cast(date_trunc('month', dexs.block_time) as date) as block_month,
    cast(date_trunc('day', dexs.block_time) as date) as block_date,
    dexs.block_time,
    dexs.block_number,
    cast(dexs.token_bought_amount_raw as uint256) as token_bought_amount_raw,
    cast(dexs.token_sold_amount_raw as uint256) as token_sold_amount_raw,
    dexs.token_bought_address,
    dexs.token_sold_address,
    dexs.taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    dexs.evt_index
from dexs
