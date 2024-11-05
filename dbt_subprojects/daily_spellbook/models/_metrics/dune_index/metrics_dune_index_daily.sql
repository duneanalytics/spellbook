{{ config(
        schema = 'metrics'
        , alias = 'dune_index_daily'
        , materialized = 'view'
        )
}}

select
    f.blockchain
    , f.block_date
    , f.fees_index
    , tr.transfers_index
    , tx.tx_index
    , (f.fees_index + tr.transfers_index + tx.tx_index) / 3 as dune_index
    , 100 as baseline
from {{ ref('metrics_fees_index_daily') }} as f
left join
    {{ ref('metrics_transfers_index_daily') }} as tr
    on f.blockchain = tr.blockchain
    and f.block_date = tr.block_date
left join
    {{ ref('metrics_transactions_index_daily') }} as tx
    on f.blockchain = tx.blockchain
    and f.block_date = tx.block_date