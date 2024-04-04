{{ config(
    schema = 'rollup_economics_ethereum',
    alias = 'l1_verification_fees',    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['name', 'hash'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "rollup_economics",
                                    \'["niftytable"]\') }}'
)}}

with verify_txns as (
  SELECT
    'zksync era' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    44*32 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb,
    t.gas_used,
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
      bytearray_substring(t.data, 1, 4) = 0x7739cbe7 -- Prove Block
      OR
      -- L1 transactions use these method ID's post-Boojum
      bytearray_substring(t.data, 1, 4) = 0x7f61885c -- Prove Batches
    )
    AND t.block_time >= timestamp '2023-03-01'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
    'polygon zkevm' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    24*32 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb,
    t.gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
  FROM {{ source('ethereum','transactions') }} AS t
  INNER JOIN {{ source('prices','usd') }} p
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.blockchain is null
    AND p.symbol = 'ETH'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE t.to = 0x5132a183e9f3cb7c848b0aac5ae0c4f0491b7ab2
    AND bytearray_substring(t.data, 1, 4) IN ( 
      0x2b0006fa, -- verifyBatchesTrustedAggregator
      0x1489ed10 -- verifyBatchesTrustedAggregator (since block 19218496)
      )
    AND t.block_time >= timestamp '2023-03-23'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
    'starkware' AS name, -- SHARPVerify used collectively by: Starknet, Sorare, ImmutableX, Apex, Myria, rhino.fi and Canvas Connect
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    456*32 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb, -- proof size might get longer with more chains
    t.gas_used,
    {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
  FROM {{ source('ethereum','transactions') }} AS t
  INNER JOIN {{ source('prices','usd') }} p
    ON p.minute = date_trunc('minute', t.block_time)
    AND p.blockchain is null
    AND p.symbol = 'ETH'
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  WHERE t.to = 0x47312450B3Ac8b5b8e247a6bB6d523e7605bDb60
    AND bytearray_substring(t.data, 1, 4) = 0x9b3b76cc -- Verify Availability Proof, imx committee
    AND t.block_time >= timestamp '2021-10-23'
    {% if is_incremental() %}
    AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

  UNION ALL 
  
  SELECT
    'scroll' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    110*32 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb,
    t.gas_used,
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
    AND bytearray_substring(t.data, 1, 4) = 0x31fa742d -- finalizeBatchWithProof
    AND t.block_time >= timestamp '2023-10-07'
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
    WHERE b.time >= timestamp '2021-10-23'
      {% if is_incremental() %}
      AND b.time >= date_trunc('day', now() - interval '7' day)
      {% endif %}
)


SELECT
  txs.name,
  txs.hash,
  txs.block_number,
  bxs.time as block_time,
  txs.proof_size_mb,
  gas_spent,
  gas_spent_usd
FROM verify_txns txs
INNER JOIN block_basefees bxs
  ON txs.block_number = bxs.block_number
