{% macro contracts_deployments(blockchain) %}

WITH contracts AS (
    SELECT
        address,
        block_time,
        block_month,
        block_number,
        tx_hash,
        code
    FROM {{ source(blockchain, 'creation_traces') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
),

--get function selectors
contracts_with_function_selectors AS (
    SELECT
        address,
        ARRAY_AGG(DISTINCT concat('0x', substr(m, 3, 8))) AS function_selectors
    FROM contracts
    CROSS JOIN UNNEST(
        --looks for pattern to extract function selectors
        regexp_extract_all(
            CAST(code AS VARCHAR),
            '63[0-9a-f]{8}14(?:60[0-9a-f]{2}|61[0-9a-f]{4}|62[0-9a-f]{6}|63[0-9a-f]{8})57'
        )
    ) AS t(m)
    GROUP BY address
),

/*
Regex explanation:
-- 63[0-9a-f]{8}      : PUSH4 <4-byte function selector>
-- 14                 : EQ
-- (?:...)            : non-capturing group for jump target
-- 60[0-9a-f]{2}      : PUSH1 <1-byte jump dest>
-- 61[0-9a-f]{4}      : PUSH2 <2-byte jump dest>
-- 62[0-9a-f]{6}      : PUSH3 <3-byte jump dest>
-- 63[0-9a-f]{8}      : PUSH4 <4-byte jump dest>
-- 57                 : JUMPI
*/

--add erc20 and erc721 flags
contracts_with_erc_flags AS (
    SELECT
        cfs.address,
        cfs.function_selectors,
        c.block_time,
        c.block_month,
        c.block_number,
        c.tx_hash,
        c.code,
        CASE
            WHEN cfs.address IN (SELECT DISTINCT contract_address FROM {{ source('tokens_' ~ blockchain, 'erc20') }}) THEN TRUE
            WHEN
                contains(cfs.function_selectors, '0xa9059cbb') -- transfer(address,uint256)
                AND contains(cfs.function_selectors, '0x70a08231') -- balanceOf(address)
            THEN TRUE
        ELSE FALSE END AS erc20_flag,
        
        CASE
            WHEN cfs.address IN (SELECT DISTINCT contract_address FROM {{ source('erc721_' ~ blockchain, 'evt_Transfer') }}) THEN TRUE
            WHEN 
                contains(cfs.function_selectors, '0x6352211e') -- ownerOf(uint256)
                AND (
                    contains(cfs.function_selectors, '0x23b872dd') -- transferFrom(address,address,uint256)
                    OR contains(cfs.function_selectors, '0x42842e0e') -- safeTransferFrom(address,address,uint256)
                    OR contains(cfs.function_selectors, '0xb88d4fde') -- safeTransferFrom(address,address,uint256,bytes)
                ) THEN TRUE
        ELSE FALSE END AS erc721_flag
    FROM contracts_with_function_selectors cfs
    LEFT JOIN contracts c ON cfs.address = c.address
)

SELECT
    '{{ blockchain }}' AS blockchain,
    address AS contract_address,
    block_time AS creation_block_time,
    block_month AS creation_block_month,
    block_number AS creation_block_number,
    tx_hash AS creation_tx_hash,
    function_selectors,
    erc20_flag,
    erc721_flag,
    code AS bytecode
FROM contracts_with_erc_flags

{% endmacro %}




