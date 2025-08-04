{% macro
    angstrom_pool_fees(asset0, asset1, fetched_bn)
%}

-- TODO generalize blockchain + addresses, (maybe use abi too?)

SELECT 
    varbinary_to_integer(varbinary_substring(data, 62, 3)) AS bundle_fee,
    varbinary_to_integer(varbinary_substring(data, 94, 3)) AS unlocked_fee,
    varbinary_to_integer(varbinary_substring(data, 126, 3)) AS protocol_unlocked_fee
FROM ethereum.logs
WHERE 
    contract_address = 0xFE77113460CF1833c4440FD17B4463f472010e10 AND 
    topic0 = 0xf325a037d71efc98bc41dc5257edefd43a1d1162e206373e53af271a7a3224e9 AND
    block_number <= fetched_bn AND 
    (varbinary_substring(topic1, 13, 20) = asset0 OR varbinary_substring(topic2, 13, 20) = asset0) AND 
    (varbinary_substring(topic1, 13, 20) = asset1 OR varbinary_substring(topic2, 13, 20) = asset1)
ORDER BY block_number DESC 
LIMIT 1


{% endmacro %}
