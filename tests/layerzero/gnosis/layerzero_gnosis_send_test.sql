WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_gnosis', 'gnosisendpoint_call_send') }}
    WHERE (call_block_number = 27960474 AND call_tx_hash = '0x4e7a1a90b6edf04f45ae5d1052c97c0bc671abd01e41385bf6a51e25e2d1e7b1')
        OR (call_block_number = 27969635 AND call_tx_hash = '0x61a92dd4f306f30ba5ce10cf7b22e5e21f0ec0c2a430465692783c3e01848f01')
        OR (call_block_number = 28016046 AND call_tx_hash = '0x8eefd856ec3ab4c45206adbf29a630baf86230c687182740bcb6c53684541279')
        OR (call_block_number = 28012050 AND call_tx_hash = '0x0e51b47e71f7c41038c9b199052575ca57e8a9097f286f10dfdf72b69560b0cb')
),

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_gnosis_send') }}
    WHERE (block_number = 27960474 AND tx_hash = '0x4e7a1a90b6edf04f45ae5d1052c97c0bc671abd01e41385bf6a51e25e2d1e7b1')
        OR (block_number = 27969635 AND tx_hash = '0x61a92dd4f306f30ba5ce10cf7b22e5e21f0ec0c2a430465692783c3e01848f01')
        OR (block_number = 28016046 AND tx_hash = '0x8eefd856ec3ab4c45206adbf29a630baf86230c687182740bcb6c53684541279')
        OR (block_number = 28012050 AND tx_hash = '0x0e51b47e71f7c41038c9b199052575ca57e8a9097f286f10dfdf72b69560b0cb')
)

SELECT *
FROM raw_data r
FULL JOIN processed_data n
    ON r.raw_block_time = n.block_time
    AND r.raw_tx_hash = n.tx_hash
    AND r.raw_trace_address = n.trace_address
    AND r.raw_destination_chain_id = n.destination_chain_id
WHERE r.raw_tx_hash IS NULL 
    OR n.tx_hash IS NULL
