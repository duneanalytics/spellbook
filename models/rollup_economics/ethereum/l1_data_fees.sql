{{ config(
    schema = 'rollup_economics_ethereum',
    alias = alias('l1_data_fees'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}

with tx_batch_appends as (
    SELECT
    'arbitrum' as name,
    t.block_time,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    length(t.data) as data_length
    FROM
    (
      SELECT
      evt_tx_hash as tx_hash,
      evt_block_time as block_time,
      evt_block_number as block_number
      FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
      WHERE evt_block_time >= timestamp '2022-01-01'
      {% if is_incremental() %}
          and evt_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}

      UNION ALL

      SELECT
      call_tx_hash as tx_hash,
      call_block_time as block_time,
      call_block_number as block_number
      FROM {{ source('arbitrum_ethereum','SequencerInbox_call_addSequencerL2BatchFromOrigin') }} o
      WHERE call_success = true
      AND call_tx_hash NOT IN
      (SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
      WHERE evt_block_time >= timestamp '2022-01-01'
      )
      {% if is_incremental() %}
          and call_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}

      UNION ALL

      SELECT
      call_tx_hash as tx_hash,
      call_block_time as block_time,
      call_block_number as block_number
      FROM {{ source('arbitrum_ethereum','SequencerInbox_call_addSequencerL2Batch') }} o
      WHERE call_success = true
      AND call_tx_hash NOT IN
      (SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
      WHERE evt_block_time >= timestamp '2022-01-01'
      )
      {% if is_incremental() %}
          and call_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}

      UNION ALL

      SELECT
      call_tx_hash as tx_hash,
      call_block_time as block_time,
      call_block_number as block_number
      FROM {{ source('arbitrum_ethereum','SequencerInbox_call_addSequencerL2BatchFromOriginWithGasRefunder') }} o
      WHERE call_success = true
      AND call_tx_hash NOT IN
      (SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
      WHERE evt_block_time >= timestamp '2022-01-01'
      )
      {% if is_incremental() %}
          and call_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    )b
    INNER JOIN {{ source('ethereum','transactions') }} t
    ON b.tx_hash = t.hash
    AND b.block_number = t.block_number
    AND b.block_time = t.block_time
    AND success = true
    AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
        and t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL
    SELECT
      op.name as name,
      t.block_time,
      t.block_number,
      t.hash,
      (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
      length(t.data) as data_length
    FROM
      {{ source('ethereum','transactions') }} as t
      INNER JOIN {{ source('dune_upload','op_stack_chain_metadata') }} op ON (
        t."from" = op.batchinbox_from_address
        AND t.to = op.batchinbox_to_address
      )
      OR (
        t."from" = op.l2_output_oracle_from_address
        AND t.to = op.l2_output_oracle
      )
    WHERE t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
        and t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

)

,block_basefees as (
    SELECT b.number as block_number, b.base_fee_per_gas, b.time
    FROM {{ source('ethereum','blocks') }} as b
    WHERE b.time >= timestamp '2022-01-01'
    {% if is_incremental() %}
        and b.time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)


SELECT
txs.name,
txs.hash,
bxs.time as block_time,
txs.data_length,
gas_spent
FROM tx_batch_appends txs
INNER JOIN block_basefees bxs
ON txs.block_number = bxs.block_number
