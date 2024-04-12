{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_batch_contracts_tx_from',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['tx_from'])
}}

WITH tagged_entities AS (
    SELECT funds_origin, entity, category
    FROM (VALUES
        (0x617c8de5bde54ffbb8d92716cc947858ca38f582, 'MEV Protocol', 'Liquid Staking')
        , (0xcDBF58a9A9b54a2C43800c50C7192946dE858321, 'Bitpanda', 'CEX')
        , (0xb10edd6fa6067dba8d4326f1c8f0d1c791594f13, 'Bitpanda', 'CEX')
        , (0x70D5cCC14a1a264c05Ff48B3ec6751b0959541aA, 'Binance US', 'CEX')
        ) 
        x (funds_origin, entity, category)
    )

, batch_contracts AS (
    SELECT contract
    FROM (VALUES
        (0x9b8c989ff27e948f55b53bb19b3cc1947852e394) -- Kiln 1
        , (0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270) -- Kiln 2
        , (0x1BDc639EaBF1c5EbC020Bb79E2dD069A8b6fe865) -- BatchDeposit
        , (0xe8239B17034c372CDF8A5F8d3cCb7Cf1795c4572) -- Batch Deposit
        ) AS temp_table (contract)
    )

SELECT txs."from" AS tx_from
, e.entity
, CONCAT(e.entity, ' ', CAST(ROW_NUMBER() OVER (PARTITION BY entity ORDER BY MIN(traces.block_time)) AS VARCHAR)) AS entity_unique_name
, e.category
FROM {{ source('eth2_ethereum', 'DepositContract_evt_DepositEvent') }} d
INNER JOIN {{ source('ethereum', 'traces') }} dep ON dep.to = 0x00000000219ab540356cbb839cbe05303d7705fa
    AND (dep.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR dep.call_type IS NULL)
    AND dep.value > UINT256 '0'
    AND dep.success
    AND dep.block_time >= TIMESTAMP '2020-10-14'
    {% if is_incremental() %}
    AND dep.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN batch_contracts bc ON bc.contract=dep."from"
INNER JOIN {{ source('ethereum', 'traces') }} traces ON traces.block_number=d.evt_block_number
    AND traces.tx_hash=d.evt_tx_hash
    AND traces.to=dep."from"
    AND (traces.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR traces.call_type IS NULL)
    AND traces.value > UINT256 '0'
    AND traces.block_time >= TIMESTAMP '2020-10-14'
    {% if is_incremental() %}
    AND traces.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
INNER JOIN tagged_entities e ON e.funds_origin=traces."from"
INNER JOIN {{ source('ethereum', 'transactions') }} txs ON txs.block_number=traces.block_number
    AND txs.hash=traces.tx_hash
    AND txs.block_time >= TIMESTAMP '2020-10-14'
    {% if is_incremental() %}
    AND txs.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
WHERE d.evt_block_time >= TIMESTAMP '2020-10-14'
{% if is_incremental() %}
AND d.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
GROUP BY 1, 2, 4