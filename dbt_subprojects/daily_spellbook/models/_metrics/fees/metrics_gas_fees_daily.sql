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