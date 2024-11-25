{% set blockchain = 'base' %}

{{ config(
        schema = 'metrics_' + blockchain
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
        , block_date
        , approx_distinct(tx_hash) as tx_count --max 2% error, which is fine
from
    {{ source('tokens', 'transfers') }}
where
    blockchain = '{{ blockchain }}'
    and amount_usd >=1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
group by
    blockchain
    , block_date
