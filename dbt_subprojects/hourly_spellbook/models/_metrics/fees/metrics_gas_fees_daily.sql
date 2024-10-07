{{ config(
        schema = 'metrics'
        , alias = 'gas_fees_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'day']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
        )
}}

select
    blockchain
    , block_date as day
    , sum(tx_fee_usd) as gas_spent_usd
from
    {{ ref('gas_fees') }}
{% if is_incremental() %}
where
    {{ incremental_predicate('block_date') }}
{% endif %}
group by
    blockchain
    , block_date