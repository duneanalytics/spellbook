{% macro
    angstrom_tx_data(
        angstrom_contract_addr, 
        blockchain
    )
%}

-- maybe use abi for log??

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM {{ source(blockchain, 'transactions') }}
WHERE to = {{ angstrom_contract_addr }} AND varbinary_substring(data, 1, 4) = 0x09c5eabe 
    AND (
        (block_number = 23084306 AND hash = 0x5f0a2eb5ea030dc3f18d03901ffe4ec161bb5fb5942e9904a3d1a75d5e6e53cc)
        OR (block_number = 23084299 AND hash = 0xd46f57a0e3aaa61a5f711cd7d2cf90f083e7e37d9125dd07e300a27d554c9c46)
        OR (block_number = 23083864 AND hash = 0x6e299e112769472208e63bd05bf40787ff9168c4731c6daa601c25b67f125d95)
    )


{% endmacro %}