{% macro yield_yak_strategies(
        blockchain = null
    )
%}

SELECT
    DISTINCT json_object('name': name, 'namespace_blockchain': CONCAT(namespace, '_{{ blockchain }}')) AS strategy
FROM {{ source(blockchain, 'contracts') }}
WHERE
    namespace = 'yield_yak'
    -- This next line ensures we're only getting the contracts which have the required Reinvest, Deposit and Withdraw tables.
    AND cardinality(filter(abi, x -> json_extract_scalar(x, '$.name') IN ('Reinvest', 'Deposit', 'Withdraw') AND json_extract_scalar(x, '$.type') = 'event')) = 3

{% endmacro %}