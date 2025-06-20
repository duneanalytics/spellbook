{{ config(
    materialized='table'
) }}

SELECT
  evt_block_time AS block_time,
  evt_tx_hash AS tx_hash,
  evt_index AS log_index,
  contract_address,
  provider,
  value,
  locktime
FROM
  {{ source('ethereum', 'VotingEscrow_evt_Deposit') }}
