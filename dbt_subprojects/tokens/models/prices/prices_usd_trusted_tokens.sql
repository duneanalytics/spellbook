{{ config(
        schema = 'prices'
        , alias = 'usd_trusted_tokens'
        , partition_by = ['month']
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'contract_address', 'minute']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.minute')]
        )
}}

select
    ptt.blockchain
    , ptt.contract_address
    , ptt.symbol
    , ptt.decimals
    , cast(date_trunc('month', block_time) as date) as month
    , p.minute
    , p.price
from
    {{ source('prices','usd_0003') }} as p
inner join
    {{ ref('prices_trusted_tokens') }} as ptt
    on p.token_id = ptt.token_id
{% if is_incremental() %}
where
    {{ incremental_predicate('p.minute') }}
{% endif %}
