{{config(
     alias = 'op_bridge_users'
)}}

WITH optimism_bridge_users AS
(SELECT DISTINCT("from") AS address, 'Optimism Bridge User' AS label
FROM (SELECT bridge.*, tx."from"
FROM {{ source('optimism', 'logs_decoded') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.tx_hash = tx.hash
WHERE bridge.contract_address = 0x4200000000000000000000000000000000000010
AND bridge.event_name = 'WithdrawalInitiated'
)
),

celer_cbridge_users AS
(SELECT DISTINCT("from") AS address, 'Celer cBridge User' AS label
FROM (SELECT bridge.*, tx."from"
FROM {{ source('celer_optimism', 'Bridge_evt_Send') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash
)
),

synapse_bridge_users AS
(SELECT DISTINCT(tx."from") AS address, 'Synapse Bridge User' AS label
FROM {{ source('synapse_optimism', 'SynapseBridge_evt_TokenMintAndSwap') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash


UNION

SELECT DISTINCT(tx."from") AS address, 'Synapse Bridge User' AS label
FROM {{ source('synapse_optimism', 'SynapseBridge_evt_TokenRedeem') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Synapse Bridge User' AS label
FROM {{ source('synapse_optimism', 'SynapseBridge_evt_TokenRedeemAndRemove') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Synapse Bridge User' AS label
FROM {{ source('synapse_optimism', 'SynapseBridge_evt_TokenRedeemAndSwap') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx 
ON bridge.evt_tx_hash = tx.hash),

hop_bridge_users AS
(SELECT DISTINCT(tx."from") AS address, 'Hop Bridge User' AS label
FROM {{ ref('hop_protocol_flows') }} bridge
JOIN {{ source('optimism', 'transactions') }} tx 
ON bridge.tx_hash = tx.hash
WHERE source_chain_name = 'Optimism'
),

across_bridge_users AS
(SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_optimism', 'OVM_OETH_BridgeDepositBox_evt_FundsDeposited')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx 
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_v2_optimism', 'UBA_Optimism_SpokePool_evt_FundsDeposited')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION  

SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_v2_optimism', 'UBA_Optimism_SpokePool_evt_OptimismTokensBridged')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_v2_optimism', 'UBA_Optimism_SpokePool_evt_TokensBridged')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_v2_optimism', 'Optimism_SpokePool_evt_FundsDeposited')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_v2_optimism', 'Optimism_SpokePool_evt_OptimismTokensBridged')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash

UNION

SELECT DISTINCT(tx."from") AS address, 'Across Bridge User' AS label
FROM {{ source('across_v2_optimism', 'Optimism_SpokePool_evt_TokensBridged')}} bridge
JOIN {{ source('optimism', 'transactions') }} tx
ON bridge.evt_tx_hash = tx.hash
),

multichain_bridge_users AS
(SELECT DISTINCT(tx_from) AS address, 'Multichain Bridge User' AS label
FROM {{ source('optimism', 'logs') }}
WHERE topic0 IN (0x409e0ad946b19f77602d6cf11d59e1796ddaa4828159a0b4fb7fa2ff6b161b79, 0x0d969ae475ff6fcaf0dcfa760d4d8607244e8d95e9bf426f8d5d69f9a3e525af)
),

combined_bridge_users AS
(SELECT *
FROM optimism_bridge_users

UNION

SELECT *
FROM celer_cbridge_users

UNION

SELECT *
FROM synapse_bridge_users

UNION

SELECT *
FROM hop_bridge_users

UNION

SELECT *
FROM across_bridge_users

UNION

SELECT *
FROM multichain_bridge_users
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'bridge' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-07' AS created_at,
    NOW() AS updated_at,
    'op_bridge_users' AS model_name,
    'persona' AS label_type
FROM
    combined_bridge_users