{{ config(
    schema = 'ton'
    , alias = 'nominators_cashflow'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['block_date', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    )
}}

WITH contracts AS (
    SELECT 
        'mj7BS8CY9rRAZMMFIiyuooAPF92oXuaoGYpwle3hDc8=' AS code_hash
        , 'tv_pool' AS pool_type
        , array[0, 4, 5] AS supported_opcodes
), pools AS (
    SELECT 
        address
        , supported_opcodes 
    FROM {{ ref('ton_accounts') }}
    JOIN contracts USING(code_hash)
)
, cashflow AS (
    SELECT 
        M.block_date
        , M.block_time
        , direction
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
            (direction = 'in' AND contains(supported_opcodes, opcode))
            OR (direction = 'out' AND opcode IS NULL)
        )
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('T.block_date') }}
    {% endif %}
)

SELECT *
FROM cashflow