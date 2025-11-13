{% macro orbiter_v1_deposits(blockchain, first_block_number) %}

WITH orbiter_eoas AS (
    SELECT orbiter_address
    FROM (VALUES
    (0x646592183ff25a0c44f09896a384004778f831ed)
    , (0x80c67432656d59144ceff962e8faf8926599bcf8)
    , (0xe4edb277e41dc89ab076a1f049f4a3efa700bce8)
    , (0xee73323912a4e3772b74ed0ca1595a152b0ef282)
    , (0xe01a40a0894970fc4c2b06f36f5eb94e73ea502d)
    , (0x41d3d33156ae7c62c094aae2995003ae63f587b3)
    , (0xd7aa9ba6caac7b0436c91396f22ca5a7f31664fc)
    , (0x0a88bc5c32b684d467b43c06d9e0899efeaf59df)
    , (0x1c84daa159cf68667a54beb412cdb8b2c193fb32)
    , (0x8086061cf07c03559fbb4aa58f191f9c4a5df2b2)
    , (0x732efacd14b0355999aebb133585787921aba3a9)
    , (0x34723b92ae9708ba33843120a86035d049da7dfa)
    , (0x095d2918b03b2e86d68551dcf11302121fb626c9)
    , (0x3bdb03ad7363152dfbc185ee23ebc93f0cf93fd1)
    , (0xacc517ea627ceb71cf25e002adaa9761623837b9)
    , (0x9c6750d463ad17deec97a630af766f0a78f95127)
    , (0xa8bd77769c875f8490e4b49f4c02a1dd83d21a18)
    , (0xa383a72e000c056cceaa9305b7b5d2d90887fbfd)
    , (0xed01d58fe6433a5fe69720a0aa0ab1d1fdb15212)
    ) AS x (orbiter_address)
    )

, blacklisted_addresses AS (
    SELECT blacklisted
    FROM (VALUES
    (0xa7883e0060286b7b9e3a87d8ef9f180a7c2673ce)
    , (0x3f5401a9d0dd2390d1a8c7060672d4b704df6372)
    , (0x0000000000000000000000000000000000008001)
    , (0xd9d74a29307cc6fc8bf424ee4217f1a587fbc8dc)
    , (0xbf3922a0cebbcd718e715e83d9187cc4bba23f11)
    , (0xabea9132b05a70803a4e85094fd0e1800777fbef)
    , (0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245)
    , (0x151409521fc4af3dbace6d97fd4148a44bf07300)
    , (0xebe80f029b1c02862b9e8a70a7e5317c06f62cae)
    , (0x44f356e8716575f2a713a3d91ae4ed1c7c054a90)
    , (0xa7883e0060286b7b9e3a87d8ef9f180a7c2673ce)
    ) AS x (blacklisted)
    )

SELECT distinct '{{blockchain}}' AS deposit_chain
, try(CAST(SUBSTRING(CAST(t.amount_raw AS VARCHAR), -4) AS bigint)) AS withdrawal_chain_id
, ci.blockchain AS withdrawal_chain
, 'Orbiter' AS bridge_name
, '1' AS bridge_version
, t.block_date AS block_date
, t.block_time AS block_time
, t.block_number AS block_number
, t.amount_raw AS deposit_amount_raw
, t."from" AS sender
, t."from" AS recipient
, t.token_standard AS deposit_token_standard
, t.contract_address AS deposit_token_address
, t.tx_from AS tx_from
, t.tx_hash AS tx_hash
, COALESCE(t.evt_index, 0) AS evt_index
, CAST(NULL AS varbinary) AS contract_address
, CAST(t.unique_key AS varchar) AS bridge_transfer_id
FROM {{ source('tokens_' + blockchain, 'transfers') }} t
INNER JOIN orbiter_eoas o ON t.to=o.orbiter_address
LEFT JOIN blacklisted_addresses b ON t."from"=b.blacklisted
INNER JOIN {{ ref('bridges_orbiter_chain_indexes') }} ci ON ci.id=try(CAST(SUBSTRING(CAST(t.amount_raw AS VARCHAR), -4) AS bigint))
WHERE b.blacklisted IS NULL
AND t.block_number >= {{first_block_number}}

{% endmacro %}{% macro orbiter_v1_deposits(blockchain, first_block_number) %}

WITH orbiter_eoas AS (
    SELECT orbiter_address
    FROM (VALUES
    (0x646592183ff25a0c44f09896a384004778f831ed)
    , (0x80c67432656d59144ceff962e8faf8926599bcf8)
    , (0xe4edb277e41dc89ab076a1f049f4a3efa700bce8)
    , (0xee73323912a4e3772b74ed0ca1595a152b0ef282)
    , (0xe01a40a0894970fc4c2b06f36f5eb94e73ea502d)
    , (0x41d3d33156ae7c62c094aae2995003ae63f587b3)
    , (0xd7aa9ba6caac7b0436c91396f22ca5a7f31664fc)
    , (0x0a88bc5c32b684d467b43c06d9e0899efeaf59df)
    , (0x1c84daa159cf68667a54beb412cdb8b2c193fb32)
    , (0x8086061cf07c03559fbb4aa58f191f9c4a5df2b2)
    , (0x732efacd14b0355999aebb133585787921aba3a9)
    , (0x34723b92ae9708ba33843120a86035d049da7dfa)
    , (0x095d2918b03b2e86d68551dcf11302121fb626c9)
    , (0x3bdb03ad7363152dfbc185ee23ebc93f0cf93fd1)
    , (0xacc517ea627ceb71cf25e002adaa9761623837b9)
    , (0x9c6750d463ad17deec97a630af766f0a78f95127)
    , (0xa8bd77769c875f8490e4b49f4c02a1dd83d21a18)
    , (0xa383a72e000c056cceaa9305b7b5d2d90887fbfd)
    , (0xed01d58fe6433a5fe69720a0aa0ab1d1fdb15212)
    ) AS x (orbiter_address)
    )

, blacklisted_addresses AS (
    SELECT blacklisted
    FROM (VALUES
    (0xa7883e0060286b7b9e3a87d8ef9f180a7c2673ce)
    , (0x3f5401a9d0dd2390d1a8c7060672d4b704df6372)
    , (0x0000000000000000000000000000000000008001)
    , (0xd9d74a29307cc6fc8bf424ee4217f1a587fbc8dc)
    , (0xbf3922a0cebbcd718e715e83d9187cc4bba23f11)
    , (0xabea9132b05a70803a4e85094fd0e1800777fbef)
    , (0xe7804c37c13166ff0b37f5ae0bb07a3aebb6e245)
    , (0x151409521fc4af3dbace6d97fd4148a44bf07300)
    , (0xebe80f029b1c02862b9e8a70a7e5317c06f62cae)
    , (0x44f356e8716575f2a713a3d91ae4ed1c7c054a90)
    , (0xa7883e0060286b7b9e3a87d8ef9f180a7c2673ce)
    ) AS x (blacklisted)
    )

SELECT '{{blockchain}}' AS deposit_chain
, try(CAST(SUBSTRING(CAST(t.amount_raw AS VARCHAR), -4) AS bigint)) AS withdrawal_chain_id
, ci.blockchain AS withdrawal_chain
, 'Orbiter' AS bridge_name
, '1' AS bridge_version
, t.block_date AS block_date
, t.block_time AS block_time
, t.block_number AS block_number
, t.amount_raw AS deposit_amount_raw
, t."from" AS sender
, t."from" AS recipient
, t.token_standard AS deposit_token_standard
, t.contract_address AS deposit_token_address
, t.tx_from AS tx_from
, t.tx_hash AS tx_hash
, COALESCE(t.evt_index, 0) AS evt_index
, CAST(NULL AS varbinary) AS contract_address
, CAST(t.unique_key AS varchar) AS bridge_transfer_id
FROM {{ source('tokens_' + blockchain, 'transfers') }} t
INNER JOIN orbiter_eoas o ON t.to=o.orbiter_address
LEFT JOIN blacklisted_addresses b ON t."from"=b.blacklisted
INNER JOIN {{ ref('bridges_orbiter_chain_indexes') }} ci ON ci.id=try(CAST(SUBSTRING(CAST(t.amount_raw AS VARCHAR), -4) AS bigint))
WHERE b.blacklisted IS NULL
AND t.block_number >= {{first_block_number}}

{% endmacro %}