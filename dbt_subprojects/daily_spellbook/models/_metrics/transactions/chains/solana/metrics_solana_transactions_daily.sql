{{ config(
        schema = 'metrics_solana'
        , alias = 'transactions_daily'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , unique_key = ['blockchain', 'block_date']
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        )
}}

select
    'solana' as blockchain
    , block_date
    , approx_distinct(tx_id) as tx_count
from
    {{ source('tokens_solana', 'transfers') }}
where
    1 = 1
    and action != 'wrap'
    and amount_usd > 1
    {% if is_incremental() %}
    and {{ incremental_predicate('block_date') }}
    {% endif %}
group by
    'solana'
    , block_date
