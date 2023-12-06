{{ config(
  tags=[ 'prod_exclude'],
  materialized='view',
  schema='integration_test', 
  alias = 'test_view') }}

with
  erc1155_ids_batch AS (
    SELECT
      *,
      CAST(evt_tx_hash AS VARCHAR) || '-' || cast(
        row_number() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) as varchar
      ) as unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}}
    CROSS JOIN UNNEST(ids) AS _u(explode_id)
      limit 100
  ),

  erc1155_values_batch AS (
    SELECT
      *,
      CAST(evt_tx_hash AS VARCHAR) || '-' || cast(
        row_number() OVER (
          PARTITION BY evt_tx_hash,
          ids
          ORDER BY
            ids
        ) as varchar
      ) as unique_transfer_id
    FROM {{source('erc1155_ethereum', 'evt_transferbatch')}}
    CROSS JOIN UNNEST("values") AS _u(explode_value)
    limit 100
  ),

  erc1155_transfers_batch AS (
    SELECT
      DISTINCT erc1155_ids_batch.explode_id,
      cast(erc1155_values_batch.explode_value as double) as explode_value,
      erc1155_ids_batch.evt_tx_hash,
      erc1155_ids_batch.to,
      erc1155_ids_batch."from",
      erc1155_ids_batch.contract_address,
      erc1155_ids_batch.evt_index,
      erc1155_ids_batch.evt_block_time
    FROM erc1155_ids_batch
      JOIN erc1155_values_batch ON erc1155_ids_batch.unique_transfer_id = erc1155_values_batch.unique_transfer_id
    limit 100
  ),

  sent_transfers as (
    select
      evt_tx_hash,
      CAST(evt_tx_hash AS VARCHAR) || '-' || cast(evt_index as varchar) || '-' || cast(to as varchar) as unique_tx_id,
      to as wallet_address,
      contract_address as token_address,
      evt_block_time,
      id as tokenId,
      cast(value as double) as amount
    FROM {{source('erc1155_ethereum', 'evt_transfersingle')}} single
    UNION ALL
    select
      evt_tx_hash,
      CAST(evt_tx_hash AS VARCHAR) || '-' || cast(evt_index as varchar) || '-' || cast(to as varchar) as unique_tx_id,
      to as wallet_address,
      contract_address as token_address,
      evt_block_time,
      explode_id as tokenId,
      cast(explode_value as double) as amount
    FROM erc1155_transfers_batch
          limit 100
  ),

  received_transfers as (
    select
      evt_tx_hash,
      CAST(evt_tx_hash AS VARCHAR) || '-' || cast(evt_index as varchar) || '-' || cast(to as varchar) as unique_tx_id,
      "from" as wallet_address,
      contract_address as token_address,
      evt_block_time,
      id as tokenId,
      - cast(value as double) as amount
    FROM {{source('erc1155_ethereum', 'evt_transfersingle')}}
    UNION ALL
    select
      evt_tx_hash,
      CAST(evt_tx_hash AS VARCHAR) || '-' || cast(evt_index as varchar) || '-' || cast(to as varchar) as unique_tx_id,
      "from" as wallet_address,
      contract_address as token_address,
      evt_block_time,
      explode_id as tokenId,
      - cast(explode_value as double) as amount
    FROM erc1155_transfers_batch
    limit 100
  )

select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, evt_tx_hash, unique_tx_id
from sent_transfers
union all
select 'ethereum' as blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, evt_tx_hash, unique_tx_id
from received_transfers
limit 100
