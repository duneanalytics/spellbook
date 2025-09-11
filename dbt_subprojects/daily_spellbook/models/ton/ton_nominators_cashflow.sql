{{ config(
        schema = 'ton_nominators'
        , alias = 'cashflow'
        , materialized = 'incremental'
        , file_format = 'delta'
        , incremental_strategy = 'merge'
        , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
        , post_hook = '{{ expose_spells(\'["ton"]\',
                                    "sector",
                                    "ton",
                                    \'["pshuvalov"]\') }}'
    )
}}

WITH contracts AS (
    -- 0 for simple commands (d, w) and sending funds by the pool
    -- 4 and 5 - special opcodes for validators
    SELECT 'mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=' AS code_hash, 
    'nominator_pool' AS pool_type,
    ARRAY[0, 4, 5] AS supported_opcodes_deposits,
    ARRAY[CAST(NULL AS BIGINT)] AS supported_opcodes_withdrawals
    
    UNION ALL

    -- 0 for withdraw command and sending funds by the pool
    -- 4096 stands for 0x1000 - OP::WITHDRAW
    SELECT 'zA05WJ6ywM/g/eKEVmV6O909lTlVrj+Y8lZkqzyQT70=' AS code_hash, 
    'single_nominator' AS pool_type,
    ARRAY[0, 4096, NULL] AS supported_opcodes_deposits,
    ARRAY[CAST(NULL AS BIGINT)] AS supported_opcodes_withdrawals

    UNION ALL

    -- the same architecture with minor changes in code
    SELECT 'pCrmnqx2/+DkUtPU8T04ehTkbAGlqtul/B2JPmxx9bo=' AS code_hash, 
    'single_nominator' AS pool_type,
    ARRAY[0, 4096, NULL] AS supported_opcodes_deposits,
    ARRAY[CAST(NULL AS BIGINT)] AS supported_opcodes_withdrawals

    UNION ALL

    -- TONWhales contracts
    -- supports plain text deposits/withdrawals and 
    -- op::stake_deposit - 2077040623
    -- op::stake_withdraw - -629129475
    -- 601104865 - old withdraw opcode
    SELECT 'XWJnptWjJbEHXAuoChKkoDx/T2dO6vKO5BZd9TUuV40=' AS code_hash, 
    'tonwhales_pool' AS pool_type,
    ARRAY[0, 2077040623, -629129475] AS supported_opcodes_deposits,
    ARRAY[0, 601104865] AS supported_opcodes_withdrawals
), pools AS (
    SELECT address, pool_type, supported_opcodes_deposits, supported_opcodes_withdrawals
    FROM {{ ref('ton_accounts') }}
    JOIN contracts USING(code_hash)
)
, cashflow AS (
    SELECT 
        M.block_date
        , M.block_time
        , direction
        , pool_type
        , CASE 
            WHEN direction = 'in' THEN destination 
            ELSE source 
          END AS pool_address
        , CASE 
            WHEN direction = 'in' THEN source 
            ELSE destination 
          END AS user_address
        , value
        , tx_hash
    FROM {{ source('ton', 'transactions') }} T 
    JOIN pools P
        ON T.account = P.address
    JOIN {{ source('ton', 'messages') }} M
        ON T.block_date = M.block_date 
        AND T.hash = M.tx_hash
        AND T.compute_exit_code = 0 
        AND T.action_result_code = 0
        AND (
            direction = 'in' AND ANY_MATCH(supported_opcodes_deposits, x -> x IS NOT DISTINCT FROM opcode)
            OR
            direction = 'out' AND ANY_MATCH(supported_opcodes_withdrawals, x -> x IS NOT DISTINCT FROM opcode)
        )
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('T.block_date') }}
    {% endif %}
)

SELECT *
FROM cashflow