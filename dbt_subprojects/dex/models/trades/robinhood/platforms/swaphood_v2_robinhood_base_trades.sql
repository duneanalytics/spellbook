{{
    config(
        schema = 'swaphood_v2_robinhood',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set swap_topic = '0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822' %}

with pair_created as (
    select
        pair,
        token0,
        token1
    from {{ source('swaphood_robinhood', 'SwapHoodFactory_evt_PairCreated') }}
    where contract_address = 0xe7206ecac3a51afe7e6179182ad4130a26068dd1
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
        and l.block_date >= date '2026-07-09'
        {% if is_incremental() %}
        and {{ incremental_predicate('l.block_time') }}
        {% endif %}
)

select
    'robinhood' as blockchain,
    'swaphood' as project,
    '2' as version,
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
