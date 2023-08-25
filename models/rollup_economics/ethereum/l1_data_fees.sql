{{ config(
    schema = 'rollup_economics_ethereum',
    alias = alias('l1_data_fees'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['name', 'hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}

with tx_batch_appends as (
    SELECT
    'arbitrum' as name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
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
      AND evt_block_time >= date_trunc('day', now() - interval '7' day)
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
      AND call_block_time >= date_trunc('day', now() - interval '7' day)
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
      AND call_block_time >= date_trunc('day', now() - interval '7' day)
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
      AND call_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    )b
    INNER JOIN {{ source('ethereum','transactions') }} t
    ON b.tx_hash = t.hash
    AND b.block_number = t.block_number
    AND success = true
    AND t.block_time >= timestamp '2022-01-01'
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
    {% if is_incremental() %}
        AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL
    SELECT
      lower(op.name) as name,
      t.block_number,
      t.hash,
      (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
      p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
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
      AND t.block_time >= timestamp '2022-01-01'
      INNER JOIN {{ source('prices','usd') }} p
        ON p.minute = date_trunc('minute', t.block_time)
        AND p.blockchain is null
        AND p.symbol = 'ETH'
    {% if is_incremental() %}
        AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'starknet' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS input_length
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND (
          t."from" = 0x2c169dfe5fbba12957bdd0ba47d9cedbfe260ca7 -- StateUpdate poster
          AND t.to = 0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4 -- StateUpdate proxy contract
          AND cast(t.data as varchar) LIKE '0x77552641%'
      )
      AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'zksync lite' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS input_length
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND
      (
      t."from" = 0xda7357bbce5e8c616bc7b0c3c86f0c71c5b4eabb -- Old L2 Operator
      OR t."from" = 0x18c208921F7a741510a7fc0CfA51E941735DAE54 -- L2 Operator
      OR t."from" = 0x01c3a1a6890a146ac187a019f9863b3ab2bff91e -- L2 Operator V1
      )
      AND t.to = 0xabea9132b05a70803a4e85094fd0e1800777fbef -- zksync
      AND cast(t.data as varchar) LIKE '0x45269298%' -- Commit Block
      AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'zksync era' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS input_length
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND t.to = 0x3dB52cE065f728011Ac6732222270b3F2360d919 -- ValidatorTimelock
      AND
      (
      cast(t.data as varchar) LIKE '0x0c4dd81%' -- Commit Block
      OR
      cast(t.data as varchar) LIKE '0xce9dcf16%' -- Execute Block
      )
      AND t.block_time >= timestamp '2023-03-01'
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'polygon zkevm' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS input_length
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND t.to = 0x5132a183e9f3cb7c848b0aac5ae0c4f0491b7ab2
      AND cast(t.data as varchar) LIKE '0x5e9145c9%' -- sequenceBatches
      AND t.block_time >= timestamp '2023-03-01'
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

)

,block_basefees as (
    SELECT b.number as block_number, b.base_fee_per_gas, b.time
    FROM {{ source('ethereum','blocks') }} as b
    WHERE b.time >= timestamp '2022-01-01'
    {% if is_incremental() %}
    AND b.time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)


SELECT
txs.name,
txs.hash,
bxs.time as block_time,
txs.data_length,
gas_spent,
gas_spent_usd
FROM tx_batch_appends txs
INNER JOIN block_basefees bxs
ON txs.block_number = bxs.block_number
