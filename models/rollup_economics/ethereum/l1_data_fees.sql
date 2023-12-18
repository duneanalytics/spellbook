{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l1_data_fees',
    
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
    length(t.data) as data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
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

    UNION ALL SELECT
      lower(op.protocol_name) as name,
      t.block_number,
      t.hash,
      (cast(t.gas_used as double) * (cast(t.gas_price as double) / 1e18)) as gas_spent,
      p.price * (cast(t.gas_used as double) * (cast(t.gas_price as double) / 1e18)) as gas_spent_usd,
      length(t.data) as data_length,
      t.gas_used,
      {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM
      {{ source('ethereum','transactions') }} as t
      INNER JOIN (
        SELECT 
            protocol_name,
            MAX(CASE WHEN submitter_type = 'L1BatchInbox' AND role_type = 'from_address' THEN address ELSE NULL END) AS "l1_batch_inbox_from_address",
            MAX(CASE WHEN submitter_type = 'L1BatchInbox' AND role_type = 'to_address' THEN address ELSE NULL END) AS "l1_batch_inbox_to_address",
            MAX(CASE WHEN submitter_type = 'L2OutputOracle' AND role_type = 'from_address' THEN address ELSE NULL END) AS "l2_output_oracle_from_address",
            MAX(CASE WHEN submitter_type = 'L2OutputOracle' AND role_type = 'to_address' THEN address ELSE NULL END) AS "l2_output_oracle_to_address"
        FROM {{ ref('addresses_ethereum_l2_batch_submitters') }}
        WHERE protocol_name IN ('OP Mainnet', 'Base', 'Public Goods Network', 'Zora', 'Aevo', 'Mode', 'Lyra', 'Orderly Network')
        GROUP BY protocol_name
      ) as op ON (
          t."from" = op.l1_batch_inbox_from_address
          AND t.to = op.l1_batch_inbox_to_address
      )
      OR (
          t."from" = op.l2_output_oracle_from_address
          AND t.to = op.l2_output_oracle_to_address
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
    (length(t.data)) AS data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
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
    'imx' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND (
            t."from" = 0x9B7f7d0d23d4CAce5A3157752D0D4e4bf25E927e -- Operator, StateUpdate poster
            AND t.to = 0x5FDCCA53617f4d2b9134B29090C87D01058e27e9 -- StateUpdate proxy contract
            AND cast(t.data as varchar) LIKE '0x538f9406%'
      )
      AND t.block_time >= timestamp '2021-03-24' -- mainnet launch date
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'zksync lite' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
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
    (length(t.data)) AS data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND (
      -- L1 transactions settle here pre-Boojum
      t.to = 0x3dB52cE065f728011Ac6732222270b3F2360d919
      -- L1 transactions settle here post-Boojum
      OR t.to = 0xa0425d71cB1D6fb80E65a5361a04096E0672De03
      )
      AND (
      -- L1 transactions use these method ID's pre-Boojum
      bytearray_substring(t.data, 1, 4) = 0x0c4dd810 -- Commit Block
      OR
      bytearray_substring(t.data, 1, 4) = 0xce9dcf16 -- Execute Block
      OR
      -- L1 transactions use these method ID's post-Boojum
      bytearray_substring(t.data, 1, 4) = 0x701f58c5 -- Commit Batches
      OR
      bytearray_substring(t.data, 1, 4) = 0xc3d93e7c -- Execute Batches
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
    (length(t.data)) AS data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
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

    UNION ALL SELECT
    'linea' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    length(t.data) as data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND t.to = 0xd19d4B5d358258f05D7B411E21A1460D11B0876F
      AND cast(t.data as varchar) LIKE '0x4165d6dd%' -- Finalize Blocks (unfortunately here the ZK proofs are also included which should rather go into table l1_verification_fees)
      AND t.block_time >= timestamp '2023-07-12'
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'scroll' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    length(t.data) as data_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND t.to = 0xa13BAF47339d63B743e7Da8741db5456DAc1E556
      AND cast(t.data as varchar) LIKE '0x1325aca0%' -- Commit Batch
      AND t.block_time >= timestamp '2023-10-07'
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
txs.block_number,
txs.data_length,
gas_spent,
gas_spent_usd,
gas_used,
calldata_gas_used
FROM tx_batch_appends txs
INNER JOIN block_basefees bxs
ON txs.block_number = bxs.block_number
