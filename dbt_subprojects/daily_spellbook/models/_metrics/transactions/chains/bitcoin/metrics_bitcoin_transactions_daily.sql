{{ config(
        schema = 'metrics_bitcoin'
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
    , approx_distinct(tx_id) as tx_count
from
    {{ source('transfers_bitcoin', 'satoshi') }}
where
    1 = 1
    and amount_transfer_usd > 1
    {% if is_incremental() or true %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
group by
    blockchain
    , block_date
