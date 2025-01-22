{%- macro yield_yak_yield_strategies(
        blockchain = null
    )
-%}

SELECT
    '{{ blockchain }}' AS blockchain
    , c.address AS contract_address
    , c.name AS contract_name
    , ct.block_time AS created_block_time
    , ct.block_number AS created_block_number
FROM {{ source(blockchain, 'contracts') }} c
INNER JOIN {{ source(blockchain, 'creation_traces') }} ct
    ON ct.address = c.address
WHERE
    c.namespace = 'yield_yak'
    -- This next line ensures we're only getting the contracts which have the required Reinvest, Deposit and Withdraw events
    -- and are therefore the types of yield strategies in which we are interested for this macro.
    AND cardinality(filter(c.abi, x -> json_extract_scalar(x, '$.name') IN ('Reinvest', 'Deposit', 'Withdraw') AND json_extract_scalar(x, '$.type') = 'event')) = 3

{%- endmacro -%}