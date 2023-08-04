{{
    config(
        alias = alias('tx_hash_labels_treasury_management_ethereum'),
    )
}}

with
  daos as (
    -- DAO list taken from dao multisig list from Dune
    -- https://github.com/duneanalytics/spellbook/blob/main/models/labels/dao/identifier/multisigs/labels_dao_multisig_ethereum.sql
    SELECT distinct address
    FROM {{ ref('labels_addresses') }}
    WHERE category = 'dao' and blockchain = 'ethereum' and label_type = 'identifier'
  ),

 treasury_management_trades as (
    select
        *
    from (
        select tx_hash, evt_index, project, version
        from {{ ref('dex_aggregator_trades') }}
        where blockchain = 'ethereum'
        and taker in (select address from daos)
        UNION ALL
        select tx_hash, evt_index, project, version
        from {{ ref('dex_trades') }}
        where blockchain = 'ethereum'
        and taker in (select address from daos)
    )
 )

select
  "ethereum" as blockchain,
  concat(tx_hash, CAST(evt_index AS VARCHAR(100)), project, version) as tx_hash_key,
  "Treasury management" AS name,
  "tx_hash" AS category,
  "gentrexha" AS contributor,
  "query" AS source,
  CAST('2023-03-01' AS TIMESTAMP) as created_at,
  now() as updated_at,
  "treasury_management" as model_name,
  "usage" as label_type
from
  treasury_management_trades
