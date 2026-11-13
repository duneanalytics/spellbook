{{ config(
    schema = 'aerodrome_base'
    , alias = 'one_tick_volume'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'pool', 'token_contract_address']
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

-- Daily token transfer volume on txs that mint or burn a one-tick Aerodrome
-- Slipstream position. Unlike the sqlmesh stablecoin spam model, this includes
-- every token transferred against the pool on those txs.

with pool_tx as (
    select distinct
        pool
        , block_date
        , tx_hash
        , block_number
    from {{ ref('aerodrome_slipstream_base_one_tick_liquidity_events') }}
    {% if is_incremental() -%}
    where {{ incremental_predicate('block_date') }}
    {% endif -%}
)

, daily_transfer_volumes as (
    select
        pool_tx.pool
        , max(transfers.block_number) as block_number
        , transfers.block_date
        , max(transfers.block_time) as block_time
        , sum(coalesce(transfers.amount, 0)) as amount
        , sum(transfers.amount_raw) as amount_raw
        , sum(coalesce(transfers.amount_usd, 0)) as amount_usd
        , count(distinct transfers."from") as unique_addresses
        , count(transfers.tx_hash) as total_transfers
        , count(distinct transfers.tx_hash) as total_transactions
        , approx_percentile(transfers.amount, 0.5) as median_transfer
        , transfers.contract_address as token_contract_address
    from {{ source('tokens', 'transfers') }} as transfers
    inner join pool_tx
        on transfers.blockchain = 'base'
        and transfers.block_date = pool_tx.block_date
        and transfers.tx_hash = pool_tx.tx_hash
        and transfers.block_number = pool_tx.block_number
        and (
            pool_tx.pool = transfers."from"
            or pool_tx.pool = transfers."to"
        )
    where transfers.blockchain = 'base'
        {% if is_incremental() -%}
        and {{ incremental_predicate('transfers.block_date') }}
        {% else -%}
        and transfers.block_date >= date '{{ project_start_date }}'
        {% endif -%}
    group by
        pool_tx.pool
        , transfers.contract_address
        , transfers.block_date
)

select
    {{ dbt_utils.generate_surrogate_key([
        'block_date'
        , 'to_hex(pool)'
        , 'to_hex(token_contract_address)'
    ]) }} as surrogate_key
    , cast('base' as varchar) as blockchain
    , cast('aerodrome' as varchar) as project
    , cast('slipstream' as varchar) as version
    , cast(date_trunc('month', block_time) as date) as block_month
    , pool
    , block_number
    , block_date
    , block_time
    , amount
    , amount_raw
    , amount_usd
    , unique_addresses
    , total_transfers
    , total_transactions
    , median_transfer
    , token_contract_address
from daily_transfer_volumes
