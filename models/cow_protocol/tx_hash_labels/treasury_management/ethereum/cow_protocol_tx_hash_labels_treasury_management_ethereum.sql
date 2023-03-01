{{
    config(
        alias='tx_hash_labels_treasury_management_ethereum',
    )
}}

with
  daos as (
    SELECT distinct address
    FROM {{ ref('labels_all') }}
    WHERE category = 'dao' and blockchain = 'ethereum' and label_type = 'identifier'
  ),

 treasury_management_trades as (
    select
        tx_hash
    from (
        select tx_hash
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and taker in (select address from daos)
        UNION ALL
        select tx_hash
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and taker in (select address from daos)
    )
 )

select
  "ethereum" as blockchain,
  tx_hash,
  "Treasury management" AS name,
  "tx_hash" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  timestamp('2023-03-01') as created_at,
  now() as updated_at,
  "treasury_management" as model_name,
  "usage" as label_type
from
  treasury_management_trades
