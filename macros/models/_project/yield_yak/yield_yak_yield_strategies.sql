{%- macro yield_yak_yield_strategies(
        blockchain = null
    )
-%}

WITH new_contracts AS (
    SELECT
        c.address AS contract_address
        , c.name AS contract_name
        , ct.block_time AS created_block_time
        , ct.block_number AS created_block_number
    FROM {{ source(blockchain, 'contracts') }} c
    {%- if is_incremental() %}
    LEFT JOIN
    {{ this }} t
        ON t.contract_address = c.address
    {%- endif %}
    INNER JOIN
    {{ source(blockchain, 'creation_traces') }} ct
        ON ct.address = c.address
    WHERE
        c.namespace = 'yield_yak'
        -- This next line ensures we're only getting the contracts which have the required Reinvest, Deposit and Withdraw events
        -- and are therefore the types of yield strategies in which we are interested for this macro.
        AND cardinality(filter(c.abi, x -> json_extract_scalar(x, '$.name') IN ('Reinvest', 'Deposit', 'Withdraw') AND json_extract_scalar(x, '$.type') = 'event')) = 3
        {%- if is_incremental() %}
        AND t.contract_address IS NULL  -- Don't want any contracts for which we already have the required information.
        AND {{ incremental_predicate('c.created_at') }}  -- This date references when the contract was added to the contracts table, not when it was created, so we can use it here in this way.
        {%- endif %}
),

new_contract_information AS (
    SELECT
        contract_address
        , contract_name
        , created_block_time
        , created_block_number
        , from_utf8(varbinary_substring({{ yield_yak_eth_call(blockchain, 'contract_address', '0x06fdde03') }}, 65)) AS name  -- function signature for name()
        , varbinary_substring({{ yield_yak_eth_call(blockchain, 'contract_address', '0xc89039c5') }}, 13) AS deposit_token_address  -- function signature for depositToken()
        , varbinary_substring({{ yield_yak_eth_call(blockchain, 'contract_address', '0xf7c618c1') }}, 13) AS reward_token_address  -- function signature for rewardToken()
    FROM new_contracts
)

SELECT
    '{{ blockchain }}' AS blockchain
    , contract_address
    , contract_name
    , created_block_time
    , created_block_number
    , name
    , {{ yield_yak_sub_wrapped_native_token(blockchain, 'deposit_token_address') }} AS deposit_token_address
    , {{ yield_yak_sub_wrapped_native_token(blockchain, 'reward_token_address') }} AS reward_token_address
FROM new_contract_information

{%- endmacro -%}