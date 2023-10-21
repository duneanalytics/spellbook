{{ config(
    schema = 'staking_ethereum',
    alias = alias('entities_batch_contracts'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pubkey'])
}}

WITH tagged_entities AS (
    SELECT funds_origin, entity, category
    FROM (VALUES
        (0x617c8de5bde54ffbb8d92716cc947858ca38f582, 'Manifold Finance', 'Staking Pool')
        ) 
        x (funds_origin, entity, category)
    )

, batch_contracts AS (
    SELECT depositor_address
    FROM (VALUES
        (0x1e68238ce926dec62b3fbc99ab06eb1d85ce0270) -- Kiln 1
        , (0x9b8c989ff27e948f55b53bb19b3cc1947852e394) -- Kiln 2
        , (0x1BDc639EaBF1c5EbC020Bb79E2dD069A8b6fe865) -- BatchDeposit
        , (0xe8239B17034c372CDF8A5F8d3cCb7Cf1795c4572) -- Batch Deposit
        ) AS temp_table (depositor_address)
    )
    
SELECT d.pubkey
, e.entity
, CONCAT(e.entity, ' ', CAST(ROW_NUMBER() OVER (PARTITION BY entity ORDER BY MIN(traces.block_time)) AS VARCHAR)) AS entity_unique_name
FROM {{ ref('staking_ethereum_deposits')}} d
INNER JOIN batch_contracts sc USING (depositor_address)
INNER JOIN {{ source('ethereum', 'traces') }} traces ON traces.block_number=d.block_number
    AND traces.tx_hash=d.tx_hash
    AND traces.to=depositor_address
    AND (traces.call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR traces.call_type IS NULL)
    AND traces.value > UINT256 '0'
INNER JOIN tagged_entities e USING e.funds_origin=traces."from"
GROUP BY 1, 2