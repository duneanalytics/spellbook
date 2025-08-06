{% macro
    angstrom_tx_data(
        angstrom_contract_addr, 
        blockchain
    )
%}

SELECT 
    block_number,
    block_time,
    hash AS tx_hash,
    index AS tx_index,
    to AS angstrom_address,
    data AS tx_data
FROM {{ source(blockchain, 'transactions') }}
WHERE to = {{ angstrom_contract_addr }} AND varbinary_substring(data, 1, 4) = 0x09c5eabe 


{% endmacro %}