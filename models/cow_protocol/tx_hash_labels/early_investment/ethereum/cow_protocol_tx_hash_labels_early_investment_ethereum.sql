{{
    config(
        alias = 'tx_hash_labels_early_investment_ethereum',
        
    )
}}

with
  project_starts as (
    select
        token_bought_address,
        min(block_date) as project_start
    from (
        select token_bought_address, block_date
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select token_bought_address, block_date
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    )
    group by
        token_bought_address

 ),

 early_investment_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version, block_date, token_bought_address
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        UNION ALL
        select tx_hash, evt_index, project, version, block_date, token_bought_address
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
    ) t join project_starts p on t.token_bought_address = p.token_bought_address
    where
        -- <=30 days deemed to be considered an early investment.
        date_diff('day', t.block_date, p.project_start) <= 30
 )

select
  'ethereum' as blockchain,
  concat(CAST(tx_hash AS VARCHAR), cast(evt_index as varchar), project, version) as tx_hash_key,
  'Early investment' AS name,
  'tx_hash' AS category,
  'gentrexha' AS contributor,
  'query' AS source,
  TIMESTAMP '2023-02-23' as created_at,
  now() as updated_at,
  'early_investment' as model_name,
  'usage' as label_type
from
  early_investment_trades
