{{ config(
        schema = 'metrics'
        , alias = 'transactions_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'day']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
        )
}}

select
    blockchain
    , date_trunc('day', block_hour) as day
    , sum(tx_count) as tx_count
from
    {{ ref('evms_transaction_metrics') }}
{% if is_incremental() %}
where
    {{ incremental_predicate('block_hour') }}
{% endif %}
group by
    blockchain
    , date_trunc('day', block_hour)