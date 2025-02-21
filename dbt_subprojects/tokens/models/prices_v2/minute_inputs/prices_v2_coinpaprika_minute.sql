{{ config(
        schema = 'prices_v2'
        , alias = 'coinpaprika_minute'
        , materialized = 'incremental'
        , file_format = 'delta'
        , partition_by = ['date']
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'contract_address', 'timestamp']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
        )
}}

select
    ptt.blockchain
    , ptt.contract_address
    , p.minute as timestamp
    , p.price
    , cast(null as double) as volume
    , 'coinpaprika' as source
    , date_trunc('day', p.minute) as date --partition
from
    {{ source('prices','usd_0003') }} as p  -- todo: fix this source
inner join
    {{ source('prices','trusted_tokens') }} as ptt
    on p.token_id = ptt.token_id
where
    1=1
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
