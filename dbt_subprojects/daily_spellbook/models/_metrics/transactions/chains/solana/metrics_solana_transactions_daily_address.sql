{% set blockchain = 'solana' %}

{{ config(
        schema = 'metrics_' + blockchain
        , alias = 'transactions_daily_address'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date', 'address']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

select
    '{{ blockchain }}' as blockchain
    , tt.block_date
    , tt.from_owner as address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , approx_distinct(tt.tx_id) as tx_count
from
    {{ source('tokens_solana', 'transfers') }} as tt
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on oa.blockchain = '{{ blockchain }}'
    and tt.from_owner = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key
where
    1 = 1
    and tt.action != 'wrap'
    and tt.amount_usd > 1
    {% if is_incremental() %}
    and {{ incremental_predicate('tt.block_date') }}
    {% endif %}
group by
    '{{ blockchain }}'
    , tt.block_date
    , tt.from_owner
    , coalesce(od.name, 'Unknown')
    , coalesce(od.primary_category, 'Uncategorized')
    , coalesce(od.country_name, 'Unknown')
