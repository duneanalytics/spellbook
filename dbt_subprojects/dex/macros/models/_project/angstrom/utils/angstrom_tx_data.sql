{% macro
    angstrom_tx_data(
        angstrom_contract_addr, 
        blockchain
    )
%}

-- maybe use abi for log??

SELECT 
    block_number,
    hash AS tx_hash,
    data AS tx_data
FROM {{ source(blockchain, 'transactions') }}
WHERE to = {{ angstrom_contract_addr }} AND varbinary_substring(data, 1, 4) = 0x09c5eabe AND hash = 0x47aefe13a19c8036c0985b59090a34adffcad108630a86aae298954554394d10


{% endmacro %}