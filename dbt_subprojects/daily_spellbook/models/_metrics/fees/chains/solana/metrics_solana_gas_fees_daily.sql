{% set blockchain = 'solana' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

with fees as (
    select
        blockchain
        , block_date
        , tx_from as address
        , sum(tx_fee_usd) as gas_fees_usd
    from
        {{ source('gas', 'fees') }}
    where
        blockchain = '{{ blockchain }}'
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , tx_from
), solana_vote_fees as (
    -- solana vote fees are stored in a different spell due to data volume & lack of value-add for materializing the fee breakdown
    select
        blockchain
        , block_date
        , tx_from as address
        , sum(tx_fee_usd) as gas_fees_usd
    from
        {{ source('gas_solana', 'vote_fees') }}
    where
        1 = 1
        {% if is_incremental() or true %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , tx_from
), combined_fees as (
    select
        fees.blockchain
        , fees.block_date
        , fees.address
        , fees.gas_fees_usd + coalesce(solana_vote_fees.gas_fees_usd, 0) as gas_fees_usd
    from
        fees
    left join
        solana_vote_fees
    on
        fees.blockchain = solana_vote_fees.blockchain
        and fees.block_date = solana_vote_fees.block_date
        and fees.address = solana_vote_fees.address
)
select
    fees.blockchain
    , fees.block_date
    , fees.address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , fees.gas_fees_usd
from combined_fees as fees
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on fees.blockchain = oa.blockchain
    and fees.address = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key