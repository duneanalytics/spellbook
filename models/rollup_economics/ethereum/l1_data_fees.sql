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
        (
          SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
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
        (
          SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
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
        (
          SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
          WHERE evt_block_time >= timestamp '2022-01-01'
        )
      {% if is_incremental() %}
      AND call_block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}

        UNION ALL

      SELECT 
      hash as tx_hash,
      block_time,
      block_number
      FROM {{ source('ethereum','transactions') }}
      WHERE "from" = 0xC1b634853Cb333D3aD8663715b08f41A3Aec47cc
      AND to = 0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6
      AND bytearray_substring(data, 1, 4) = 0x3e5aa082 --addSequencerL2BatchFromBlobs
      AND block_number >= 19433943 --when arbitrum started submitting blobs
      {% if is_incremental() %}
      AND block_time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
  ) b
  INNER JOIN {{ source('ethereum','transactions') }} t
    ON b.tx_hash = t.hash
    AND b.block_number = t.block_number
    AND t.success = true
    AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  INNER JOIN {{ source('prices','usd') }} p
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.blockchain is null
    AND p.symbol = 'ETH'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
    lower(protocol_name) as name,
    block_number,
    hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    data_length,
    gas_used,
    calldata_gas_used
  FROM (
    SELECT protocol_name, t.block_time, t.block_number, t.hash, t.gas_used, t.gas_price, length(t.data) as data_length, {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM {{ source('ethereum','transactions') }} as t
    INNER JOIN {{ ref('addresses_ethereum_optimism_batchinbox_combinations') }} as op 
      ON t."from" = op.l1_batch_inbox_from_address
         AND t.to = op.l1_batch_inbox_to_address
      WHERE t.block_time >= timestamp '2020-01-01'
      UNION ALL
    SELECT protocol_name, t.block_time, t.block_number, t.hash, t.gas_used, t.gas_price, length(t.data) as data_length, {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    FROM {{ source('ethereum','transactions') }} as t
    INNER JOIN {{ ref('addresses_ethereum_optimism_outputoracle_combinations') }} as op 
      ON t."from" = op.l2_output_oracle_from_address
         AND t.to = op.l2_output_oracle_to_address
      WHERE t.block_time >= timestamp '2020-01-01'
    ) b
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', b.block_time)
      AND p.blockchain is null
      AND p.symbol = 'ETH'
      AND p.minute >= timestamp '2020-01-01'
      {% if is_incremental() %}
      AND p.minute >= date_trunc('day', now() - interval '7' day)
      {% endif %}
    {% if is_incremental() %}
    WHERE b.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
    
  SELECT
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE (
      t.to = 0xc662c410C0ECf747543f5bA90660f6ABeBD9C8c4 -- StateUpdate proxy contract
      AND bytearray_substring(t.data, 1, 4) = 0x77552641 -- updateState
    )
    AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL 
    
  SELECT
    'imx' AS chain, -- imx state updates to L1 through the Data Availability Committee, imx uses offchain DA
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE (
        (t.to = 0x5FDCCA53617f4d2b9134B29090C87D01058e27e9 OR t.to = 0x16BA0f221664A5189cf2C1a7AF0d3AbFc70aA295)
        AND (bytearray_substring(t.data, 1, 4) = 0x538f9406 OR bytearray_substring(t.data, 1, 4) = 0x504f7f6f) -- StateUpdate & Verify Availability Proof
    )
    AND t.block_time >= timestamp '2021-03-24' -- mainnet launch date
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE
    (
      t."from" = 0xda7357bbce5e8c616bc7b0c3c86f0c71c5b4eabb -- Old L2 Operator
      OR t."from" = 0x18c208921F7a741510a7fc0CfA51E941735DAE54 -- L2 Operator
      OR t."from" = 0x01c3a1a6890a146ac187a019f9863b3ab2bff91e -- L2 Operator V1
    )
    AND t.to = 0xabea9132b05a70803a4e85094fd0e1800777fbef -- zksync
    AND bytearray_substring(t.data, 1, 4) = 0x45269298 -- Commit Block
    AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE (
      -- L1 transactions settle here pre-Boojum
      t.to = 0x3dB52cE065f728011Ac6732222270b3F2360d919
      -- L1 transactions settle here post-Boojum
      OR t.to = 0xa0425d71cB1D6fb80E65a5361a04096E0672De03
      -- L1 transactions settle here post-EIP4844
      OR t.to = 0xa8CB082A5a689E0d594d7da1E2d72A3D63aDc1bD
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

  UNION ALL 
  
  SELECT
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE 
    (
      t.to = 0x5132a183e9f3cb7c848b0aac5ae0c4f0491b7ab2 -- old proxy 
      OR t.to = 0x519E42c24163192Dca44CD3fBDCEBF6be9130987 -- new proxy (as of block 19218878)
    )
    AND bytearray_substring(t.data, 1, 4) IN (
      0x5e9145c9, -- sequenceBatches
      0xecef3f99 -- sequenceBatches (as of block 19218878)
      )
    AND t.block_time >= timestamp '2023-03-01'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE t.to = 0xd19d4B5d358258f05D7B411E21A1460D11B0876F -- Linea, L1 Message Service
    AND bytearray_substring(t.data, 1, 4) IN (
      0x4165d6dd, -- Finalize Blocks (proof verified immediately)
      0xd630280f, -- finalizeCompressedBlocksWithProof (Aplha v2 Release at block. 19222438)
      0x7a776315 -- submitData (Aplha v2 Release at block. 19222438)
      )
    AND t.block_time >= timestamp '2023-07-12'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
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
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE t.to = 0xa13BAF47339d63B743e7Da8741db5456DAc1E556
    AND bytearray_substring(t.data, 1, 4) = 0x1325aca0 -- Commit Batch
    AND t.block_time >= timestamp '2023-10-07'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
    'loopring' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS input_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
  FROM {{ source('ethereum','transactions') }} AS t
  INNER JOIN {{ source('prices','usd') }} p
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.blockchain is null
    AND p.symbol = 'ETH'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE t.to = 0x153CdDD727e407Cb951f728F24bEB9A5FaaA8512
    AND bytearray_substring(t.data, 1, 4) = 0xdcb2aa31 -- submitBlocksWithCallbacks (proof verified immediately)
    AND t.block_time >= timestamp '2021-03-23' 
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
    'Mantle' AS chain,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    (length(t.data)) AS input_length,
    gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
  FROM {{ source('ethereum','transactions') }} AS t
  INNER JOIN {{ source('prices','usd') }} p
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.blockchain is null
    AND p.symbol = 'ETH'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE t.to = 0xD1328C9167e0693B689b5aa5a024379d4e437858
    AND bytearray_substring(t.data, 1, 4) = 0x49cd3004 -- createAssertionWithStateBatch 
    AND t.block_time >= timestamp '2023-06-27' 
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)

,block_basefees as (
    SELECT 
      b.number as block_number
      , b.base_fee_per_gas
      , b.time
    FROM {{ source('ethereum','blocks') }} as b
    WHERE b.time >= timestamp '2021-03-23'
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
