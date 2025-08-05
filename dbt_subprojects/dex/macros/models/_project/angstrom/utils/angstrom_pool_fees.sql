{% macro
    angstrom_pool_fees(
        angstrom_contract_addr, 
        blockchain
    )
%}

-- maybe use abi for log??

SELECT 
    varbinary_to_integer(varbinary_substring(l.data, 62, 3)) AS bundle_fee,
    varbinary_to_integer(varbinary_substring(l.data, 94, 3)) AS unlocked_fee,
    varbinary_to_integer(varbinary_substring(l.data, 126, 3)) AS protocol_unlocked_fee,
    topic1,
    topic2
FROM {{ source(blockchain, 'logs') }} AS l
WHERE 
    contract_address = {{ angstrom_contract_addr }} AND 
    topic0 = 0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9
ORDER BY block_number DESC 
LIMIT 1


{% endmacro %}