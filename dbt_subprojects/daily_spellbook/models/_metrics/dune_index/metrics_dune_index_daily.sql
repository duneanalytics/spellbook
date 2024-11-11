{{ config(
        schema = 'metrics'
        , alias = 'dune_index_daily'
        , materialized = 'view'
        )
}}

select
    blockchain
    , block_date
    , coalesce(f.fees_index,0) as fees_index
    , coalesce(tr.transfers_index,0) as transfers_index
    , coalesce(tx.tx_index,0) as tx_index
    , coalesce(f.fees_index,0)*0.45 + coalesce(tr.transfers_index,0)*0.45 + coalesce(tx.tx_index,0)*0.10 as dune_index
from {{ ref('metrics_fees_index_daily') }} as f
left join
    {{ ref('metrics_transfers_index_daily') }} as tr
    using (blockchain, block_date)
left join
    {{ ref('metrics_transactions_index_daily') }} as tx
     using (blockchain, block_date)