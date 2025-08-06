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
WHERE block_number = 23077861 AND to = {{ angstrom_contract_addr }} AND varbinary_substring(data, 1, 4) = 0x09c5eabe AND hash = 0x32716081b3461e4f4770e14d97565c003aecf647837d151a8380f6b9722e7faf


{% endmacro %}