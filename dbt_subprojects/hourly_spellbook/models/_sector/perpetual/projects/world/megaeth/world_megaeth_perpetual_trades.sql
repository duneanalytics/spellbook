{{ config(
    schema = 'world_megaeth',
    alias = 'perpetual_trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set project_start_date = '2026-02-01' %}
{% set composite_exchange = '0x5e3ae52eba0f9740364bd5dd39738e1336086a8b' %}

with perp_events as (
    select
        e.evt_block_time as block_time
        , e.evt_block_number as block_number
        , e.evt_tx_hash as tx_hash
        , e.evt_index
        , e.contract_address as market_address
        , e.evt_tx_from as trader
        , e.evt_tx_from as tx_from
        , e.evt_tx_to as tx_to
    from {{ source('world_megaeth', 'compositeexchange_evt_perporderbookregistered') }} e
    where e.contract_address = {{ composite_exchange }}
    {% if is_incremental() %}
        and {{ incremental_predicate('e.evt_block_time') }}
    {% else %}
        and e.evt_block_time >= timestamp '{{ project_start_date }}'
    {% endif %}
)

select
    'megaeth' as blockchain
    , cast(date_trunc('day', p.block_time) as date) as block_date
    , cast(date_trunc('month', p.block_time) as date) as block_month
    , p.block_time
    , cast(null as varchar) as virtual_asset
    , cast(null as varchar) as underlying_asset
    , cast(null as varchar) as market
    , p.market_address
    , cast(null as double) as volume_usd
    , cast(null as double) as fee_usd
    , cast(null as double) as margin_usd
    , cast('open' as varchar) as trade
    , 'world' as project
    , '1' as version
    , 'world' as frontend
    , p.trader
    , cast(null as uint256) as volume_raw
    , p.tx_hash
    , p.tx_from
    , p.tx_to
    , p.evt_index
from perp_events p
