{% set blockchain = 'tron' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'gas_fees_daily_address'
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
        {% if is_incremental() %}
        and {{ incremental_predicate('block_date') }}
        {% endif %}
    group by
        blockchain
        , block_date
        , tx_from
)

select
    fees.blockchain
    , fees.block_date
    , fees.address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , fees.gas_fees_usd * coalesce(t.trx_fee_ratio,0.0) as gas_fees_usd -- apply correction to account for subsidized fees
from fees
left join {{ref('tron_fee_correction')}} t
    on fees.block_date = t.day
    and fees.blockchain = '{{ blockchain }}'
    {% if is_incremental() %}
    and {{ incremental_predicate('t.day') }}
    {% endif %}
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on fees.blockchain = oa.blockchain
    and fees.address = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key