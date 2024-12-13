{% macro metrics_transactions_evm(blockchain) %}

select
    blockchain
    , block_date
    , approx_distinct(tx_hash) as tx_count --max 2% error, which is fine
from
    {{ source('tokens', 'transfers') }}
where
    blockchain = '{{ blockchain }}'
    and amount_usd >= 1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
group by
    blockchain
    , block_date

{% endmacro %}

{# ########################################################################## #}

{% macro metrics_transactions_evm_address(blockchain) %}

select
    tt.blockchain
    , tt.block_date
    , tt.tx_from as address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , approx_distinct(tt.tx_hash) as tx_count --max 2% error, which is fine
from
    {{ source('tokens', 'transfers') }} as tt
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on tt.blockchain = oa.blockchain
    and tt.tx_from = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key
where
    tt.blockchain = '{{ blockchain }}'
    and tt.amount_usd >= 1
    {% if is_incremental() %}
    and {{ incremental_predicate('tt.block_date') }}
    {% endif %}
group by
    tt.blockchain
    , tt.block_date
    , tt.tx_from
    , coalesce(od.name, 'Unknown')
    , coalesce(od.primary_category, 'Uncategorized')
    , coalesce(od.country_name, 'Unknown')

{% endmacro %}