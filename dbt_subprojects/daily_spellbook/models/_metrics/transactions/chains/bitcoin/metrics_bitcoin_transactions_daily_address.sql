{% set blockchain = 'bitcoin' %}

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
    tt.blockchain
    , tt.block_date
    , case
        when substring(tt.wallet_address, 1, 3) = 'bc1' then cast(tt.wallet_address as varbinary) --we don't have bech32() function for this address type
        else from_base58(tt.wallet_address) --all other address types *should* be fine to use base58
    end as address
    , coalesce(od.name, 'Unknown') as name
    , coalesce(od.primary_category, 'Uncategorized') as primary_category
    , coalesce(od.country_name, 'Unknown') as hq_country
    , approx_distinct(tt.tx_id) as tx_count
from
    {{ source('transfers_bitcoin', 'satoshi') }} as tt
left join
    {{ source('labels', 'owner_addresses') }} as oa
    on oa.blockchain = '{{ blockchain }}'
    and tt.wallet_address = oa.address
left join
    {{ source('labels', 'owner_details') }} as od
    on oa.owner_key = od.owner_key
where
    tt.wallet_address is not null --address can be null on coinbase transactions
    and tt.amount_transfer_usd > 1
    {% if is_incremental() %}
    and {{ incremental_predicate('tt.block_date') }}
    {% endif %}
group by
    tt.blockchain
    , tt.block_date
    , tt.wallet_address
    , coalesce(od.name, 'Unknown')
    , coalesce(od.primary_category, 'Uncategorized')
    , coalesce(od.country_name, 'Unknown')
