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

with fees as (
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
), solana_vote_fees as (
    -- solana vote fees are stored in a different spell due to data volume & lack of value-add for materializing the fee breakdown
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
), combined_fees as (
    select
        fees.blockchain
        , fees.block_date
        , fees.gas_fees_usd + coalesce(solana_vote_fees.gas_fees_usd, 0) as gas_fees_usd
    from
        fees
    left join
        solana_vote_fees
    on
        fees.blockchain = solana_vote_fees.blockchain
        and fees.block_date = solana_vote_fees.block_date
)


select
    blockchain
    ,block_date
    ,case when blockchain = 'tron'
        then gas_fees_usd * coalesce(t.trx_fee_ratio,0.0) -- apply correction to account for subsidized fees
        else gas_fees_usd end
        as gas_fees_usd
from combined_fees
left join {{ref('tron_fee_correction')}} t
    on block_date = t.day
    and blockchain = 'tron'
    {% if is_incremental() %}
    and {{ incremental_predicate('day') }}
    {% endif %}
