{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_contracts',
    
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

WITH contracts AS (
    SELECT address, entity, entity AS entity_unique_name, category, 'deposit_address' AS tagging_method
    FROM
    (VALUES
    (0xdcd51fc5cd918e0461b9b7fb75967fdfd10dae2f, 'Rocket Pool', 'Liquid Staking')
    , (0x1cc9cf5586522c6f483e84a19c3c2b0b6d027bf0, 'Rocket Pool', 'Liquid Staking')
    , (0x2fb42ffe2d7df8381853e96304300c6a5e846905, 'Rocket Pool', 'Liquid Staking')
    , (0x9304b4ebfbe68932cf9af8de4d21d7e7621f701a, 'Rocket Pool', 'Liquid Staking')
    --, (0x9b8c989ff27e948f55b53bb19b3cc1947852e394, 'Kiln', 'Staking Pool') -- Kiln doesn't custody or manage any validators, hence the removal
    --, (0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270, 'Kiln', 'Staking Pool')
    , (0x2421a0af8badfae12e1c1700e369747d3db47b09, 'SenseiNode', 'Staking Pool')
    , (0x10e02a656b5f9de2c44c687787c36a2c4801cc40, 'Tranchess', 'Liquid Staking')
    , (0x447c3ee829a3b506ad0a66ff1089f30181c42637, 'HashKing', 'Liquid Staking')
    , (0xa8f50a6c41d67685b820b4fe6bed7e549e54a949, 'Eth2Stake', 'Staking Pool')
    , (0xf243a92eb7d4b4f6a00a57888b887bd01ec6fd12, 'MyEtherWallet', 'Staking Pool')
    , (0x73fd39ba4fb23c9b080fca0fcbe4c8c7a2d630d0, 'MyEtherWallet', 'Staking Pool')
    , (0xe7b385fb5d81259280b7d639df81513ab8b005e4, 'MyEtherWallet', 'Staking Pool')
    , (0x82ce843130ff0ae069c54118dfbfa6a5ea17158e, 'Gemini', 'CEX')
    , (0x24d729aae93a05a729e68504e5ccdfa3bb876491, 'Gemini', 'CEX')
    , (0xcf5ea1b38380f6af39068375516daf40ed70d299, 'Stader', 'Liquid Staking')
    , (0x4f4bfa0861f62309934a5551e0b2541ee82fdcf1, 'Stader', 'Liquid Staking')
    , (0x09134c643a6b95d342bdaf081fa473338f066572, 'Stader', 'Liquid Staking')
    , (0xd1a72bd052e0d65b7c26d3dd97a98b74acbbb6c5, 'Stader', 'Liquid Staking')
        ) 
        x (address, entity, category)
    )

SELECT address AS depositor_address
, entity
, CONCAT(entity, ' ', CAST(ROW_NUMBER() OVER (PARTITION BY entity ORDER BY first_used) AS VARCHAR)) AS entity_unique_name
, category AS category
FROM (
    SELECT traces."from" AS address
    , c.entity
    , c.category
    , MIN(txs.block_time) AS first_used
    FROM {{ source('ethereum', 'transactions') }} txs
    INNER JOIN {{ source('ethereum', 'traces') }} traces
        ON txs.hash=traces.tx_hash 
        AND traces.to = 0x00000000219ab540356cbb839cbe05303d7705fa
        {% if not is_incremental() %}
        AND traces.block_time >= DATE '2020-10-14'
        {% endif %}
        {% if is_incremental() %}
        AND traces.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    INNER JOIN contracts c ON c.address=txs.to
    WHERE txs.to IN (SELECT address FROM contracts)
        {% if not is_incremental() %}
        AND txs.block_time >= DATE '2020-10-14'
        {% endif %}
        {% if is_incremental() %}
        AND txs.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    GROUP BY 1, 2, 3
    )
