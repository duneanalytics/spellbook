{% macro metrics_fees_evm(blockchain) %}

select
    fees.blockchain
    , fees.block_date
    , fees.tx_from as address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , sum(fees.tx_fee_usd) as gas_fees_usd
from
    {{ source('gas', 'fees') }} as fees
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on fees.blockchain = oa.blockchain
    and fees.tx_from = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key
where
    fees.blockchain = '{{blockchain}}'
{% if is_incremental() or true %}
    and {{ incremental_predicate('fees.block_date') }}
{% endif %}
group by
    fees.blockchain
    , fees.block_date
    , fees.tx_from
    , coalesce(od.name, 'Unknown')
    , coalesce(od.primary_category, 'Uncategorized')
    , coalesce(od.country_name, 'Unknown')
{% endmacro %}