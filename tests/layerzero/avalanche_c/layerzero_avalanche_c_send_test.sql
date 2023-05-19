WITH raw_data AS (
    SELECT call_block_time AS raw_block_time,
        call_tx_hash AS raw_tx_hash,
        call_trace_address AS raw_trace_address,
        _dstChainId AS raw_destination_chain_id
    FROM {{ source ('layerzero_avalanche_c', 'Endpoint_call_send') }}
    WHERE (call_block_number = 30200438 AND call_tx_hash = '0xd60a45d481a514923e6502eef902138fb6dbf40c60f1f65c171e5f055bee1c00')
        OR (call_block_number = 30201252 AND call_tx_hash = '0x3f937d16b7d7f8f66589d85c5e7046f72ecc9fc5758d0210591dbacb6d35ba21')
        OR (call_block_number = 30199350 AND call_tx_hash = '0xd94ad305cd2938f0be1335360de7a93198f6574bb4461c7f8df1bc3e6721d028')
        OR (call_block_number = 30202595 AND call_tx_hash = '0xf6816fd51546c66f914bc845257edd28eae6a83865aa8f8cb818067e1d0c4650')
),

processed_data AS (
    SELECT block_time,
        tx_hash,
        trace_address,
        destination_chain_id
    FROM {{ ref('layerzero_avalanche_c_send') }}
    WHERE (block_number = 30200438 AND tx_hash = '0xd60a45d481a514923e6502eef902138fb6dbf40c60f1f65c171e5f055bee1c00')
        OR (block_number = 30201252 AND tx_hash = '0x3f937d16b7d7f8f66589d85c5e7046f72ecc9fc5758d0210591dbacb6d35ba21')
        OR (block_number = 30199350 AND tx_hash = '0xd94ad305cd2938f0be1335360de7a93198f6574bb4461c7f8df1bc3e6721d028')
        OR (block_number = 30202595 AND tx_hash = '0xf6816fd51546c66f914bc845257edd28eae6a83865aa8f8cb818067e1d0c4650')
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
