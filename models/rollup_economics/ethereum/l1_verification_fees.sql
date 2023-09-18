{{ config(
    schema = 'rollup_economics_ethereum',
    alias = alias('l1_verification_fees'),
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

with verify_txns as (
    SELECT
    'zksync era' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    1408 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND t.to = 0x3dB52cE065f728011Ac6732222270b3F2360d919 -- ValidatorTimelock
      AND cast(t.data as varchar) LIKE '0x7739cbe7%' -- Prove Block
      AND t.block_time >= timestamp '2023-03-01'
      AND p.blockchain is null
      AND p.symbol = 'ETH'
    {% if is_incremental() %}
       AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'polygon zkevm' AS name,
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    768 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND t.to = 0x5132a183e9f3cb7c848b0aac5ae0c4f0491b7ab2
      AND cast(t.data as varchar) LIKE '0xa50a164b%' -- proveBatches
      AND t.block_time >= timestamp '2023-03-01'
      AND p.blockchain is null
      AND p.symbol = 'ETH'
    {% if is_incremental() %}
      AND t.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION ALL SELECT
    'imx' AS name, -- not included here is the SHARP Verifier [0x47312450B3Ac8b5b8e247a6bB6d523e7605bDb60] as all Starkware chains share it together
    t.block_number,
    t.hash,
    (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent,
    p.price * (cast(gas_used as double) * (cast(gas_price as double) / 1e18)) as gas_spent_usd,
    768 / cast(1024 AS double) / cast(1024 AS double) AS proof_size_mb
    FROM {{ source('ethereum','transactions') }} AS t
    INNER JOIN {{ source('prices','usd') }} p
      ON p.minute = date_trunc('minute', t.block_time)
      AND t.to = 0x16BA0f221664A5189cf2C1a7AF0d3AbFc70aA295
      AND cast(t.data as varchar) LIKE '0x504f7f6f%' -- Verify Availability Proof, imx committee
      AND t.block_time >= timestamp '2021-03-24'
      AND p.blockchain is null
      AND p.symbol = 'ETH'
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
txs.proof_size_mb,
gas_spent,
gas_spent_usd
FROM verify_txns txs
INNER JOIN block_basefees bxs
ON txs.block_number = bxs.block_number
