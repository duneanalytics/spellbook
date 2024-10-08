{{ config(
        schema = 'metrics'
        , alias = 'transactions_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

select
    blockchain
    , date_trunc('day', block_hour) as block_date
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