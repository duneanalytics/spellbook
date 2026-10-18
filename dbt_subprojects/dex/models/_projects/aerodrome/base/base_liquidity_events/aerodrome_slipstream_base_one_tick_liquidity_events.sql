{{ config(
    schema = 'aerodrome_slipstream_base'
    , alias = 'one_tick_liquidity_events'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'tx_hash', 'evt_index', 'event_type']
    , partition_by = ['block_date']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , post_hook='{{ expose_spells(
        blockchains = \'["base"]\',
        spell_type = "project",
        spell_name = "aerodrome",
        contributors = \'["ryeblocks"]\'
    ) }}'
    )
}}

{% set project_start_date = '2024-04-24' %}

-- Aerodrome Slipstream positions whose width is exactly one tick:
-- abs(tickLower - tickUpper) = factory.tickSpacing.
-- Not limited to stablecoins; every token pair that uses this methodology is included.
-- Kept out of aerodrome_base_base_liquidity_events so these rows are not double-counted.

with pools as (
    select
        pool
        , token0
        , token1
        , tickSpacing as tick_spacing
    from {{ source('aerodrome_base', 'CLFactory_evt_PoolCreated') }}
)

, mint as (
    select
        mint.evt_block_date as block_date
        , mint.evt_block_time as block_time
        , mint.evt_block_number as block_number
        , mint.evt_tx_hash as tx_hash
        , mint.evt_tx_from as tx_from
        , mint.evt_tx_to as tx_to
        , mint.evt_index
        , cast('mint' as varchar) as event_type
        , mint.contract_address as pool
        , pools.token0
        , pools.token1
        , pools.tick_spacing
        , mint.tickLower as tick_lower
        , mint.tickUpper as tick_upper
        , mint.amount as liquidity
        , mint.amount0 as amount0_raw
        , mint.amount1 as amount1_raw
        , mint.owner
        , mint.sender
    from {{ source('aerodrome_base', 'CLPool_evt_Mint') }} as mint
    inner join pools
        on pools.pool = mint.contract_address
    where abs(mint.tickLower - mint.tickUpper) = pools.tick_spacing
        {% if is_incremental() -%}
        and {{ incremental_predicate('mint.evt_block_date') }}
        {% else -%}
        and mint.evt_block_date >= date '{{ project_start_date }}'
        {% endif -%}
)

, burn as (
    select
        burn.evt_block_date as block_date
        , burn.evt_block_time as block_time
        , burn.evt_block_number as block_number
        , burn.evt_tx_hash as tx_hash
        , burn.evt_tx_from as tx_from
        , burn.evt_tx_to as tx_to
        , burn.evt_index
        , cast('burn' as varchar) as event_type
        , burn.contract_address as pool
        , pools.token0
        , pools.token1
        , pools.tick_spacing
        , burn.tickLower as tick_lower
        , burn.tickUpper as tick_upper
        , burn.amount as liquidity
        , burn.amount0 as amount0_raw
        , burn.amount1 as amount1_raw
        , burn.owner
        , burn.evt_tx_from as sender
    from {{ source('aerodrome_base', 'CLPool_evt_Burn') }} as burn
    inner join pools
        on pools.pool = burn.contract_address
    where abs(burn.tickLower - burn.tickUpper) = pools.tick_spacing
        {% if is_incremental() -%}
        and {{ incremental_predicate('burn.evt_block_date') }}
        {% else -%}
        and burn.evt_block_date >= date '{{ project_start_date }}'
        {% endif -%}
)

, events as (
    select * from mint
    union all
    select * from burn
)

select
    {{ dbt_utils.generate_surrogate_key([
        'block_date'
        , 'to_hex(tx_hash)'
        , 'evt_index'
        , 'event_type'
    ]) }} as surrogate_key
    , cast('base' as varchar) as blockchain
    , cast('aerodrome' as varchar) as project
    , cast('slipstream' as varchar) as version
    , cast(date_trunc('month', block_time) as date) as block_month
    , block_date
    , block_time
    , block_number
    , tx_hash
    , tx_from
    , tx_to
    , evt_index
    , event_type
    , pool
    , pool as id
    , token0
    , token1
    , tick_spacing
    , tick_lower
    , tick_upper
    , liquidity
    , amount0_raw
    , amount1_raw
    , owner
    , sender
from events
