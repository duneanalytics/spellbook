{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with evm_fees as (
    select
        blockchain
        , block_date
        , sum(tx_fee_usd) as gas_fees_usd
    from
        {{ source('gas', 'fees') }}
    {% if is_incremental() %}
    where
        {{ incremental_predicate('block_date') }}
    {% endif %}
    group by
        blockchain
        , block_date
), solana_regular_fees as (
    select
        blockchain
        , block_date
        , sum(tx_fee_usd) as gas_fees_usd
    from
        {{ source('gas_solana', 'fees') }}
    {% if is_incremental() %}
    where
        {{ incremental_predicate('block_date') }}
    {% endif %}
    group by
        blockchain
        , block_date
), solana_vote_fees as (
    select
        blockchain
        , block_date
        , sum(tx_fee_usd) as gas_fees_usd
    from
        {{ source('gas_solana', 'vote_fees') }}
    {% if is_incremental() %}
    where
        {{ incremental_predicate('block_date') }}
    {% endif %}
    group by
        blockchain
        , block_date
), solana_fees as (
    select
        srf.blockchain
        , srf.block_date
        , srf.gas_fees_usd + svf.gas_fees_usd as gas_fees_usd
    from
        solana_regular_fees as srf
    inner join
        solana_vote_fees as svf
    on
        srf.block_date = svf.block_date
        and srf.blockchain = svf.blockchain
)
select
    *
from
    evm_fees
union all
select
    *
from
    solana_fees