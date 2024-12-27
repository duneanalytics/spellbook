{{ config(
        schema = 'prices_v2'
        , alias = 'coinpaprika_minute'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'contract_address', 'minute']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.timestamp')]
        )
}}

select
    ptt.blockchain
    , ptt.contract_address
    , p.minute as timestamp
    , p.price
    , null as volume
    , 'coinpaprika' as source
from
    {{ source('prices','usd') }} as p
inner join
    {{ ref('prices_trusted_tokens') }} as ptt
    on p.token_id = ptt.token_id
{% if is_incremental() %}
where
    {{ incremental_predicate('p.minute') }}
{% endif %}
