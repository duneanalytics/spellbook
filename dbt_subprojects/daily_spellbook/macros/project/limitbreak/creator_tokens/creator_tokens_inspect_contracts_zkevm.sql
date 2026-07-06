{% macro creator_tokens_inspect_contracts_zkevm(blockchain) %}

WITH clones AS (
    SELECT 
        true as is_clone, 
        block_time as creation_time, 
        address, 
        varbinary_substring(code, 13, 20) AS implementation_address,
        CAST(date_trunc('day', block_time) as date)  as block_date,
        CAST(date_trunc('month', block_time) as date)  as block_month
    FROM 
        {{ source(blockchain, 'creation_traces') }}
    WHERE
        length(code) = 32
        {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
        {% endif %}
), deploys AS (
    SELECT 
        false as is_clone, 
        block_time as creation_time, 
        varbinary_substring(output, 13, 20) AS address,
        varbinary_substring(input, 37, 32) AS bytecode_hash,
        CAST(date_trunc('day', block_time) as date)  as block_date,
        CAST(date_trunc('month', block_time) as date)  as block_month
    FROM 
        {{ source(blockchain, 'traces') }}
    WHERE 
        to = 0x0000000000000000000000000000000000008006
        AND (varbinary_position(input, 0x9c4d535b) = 1 OR varbinary_position(input, 0x3cda3351) = 1)
        AND length(input) > 104
        AND length(output) = 32
        {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
        {% endif %}
-- PERF (CUR2-2717): The erc721 and erc1155 implementation-registration lookups
-- share the same base predicate (`to = 0x...800e AND varbinary_position(input,
-- 0xf5e69a47) = 1`), so we unify them into a single scan of `traces`. The
-- resulting rows are tagged with per-standard booleans and fanned out via
-- CROSS JOIN UNNEST, preserving the original behavior for dual-standard
-- contracts. Impls are intentionally scanned without an incremental predicate:
-- a clone in the current window can reference an implementation registered
-- arbitrarily far in the past. Merging the two impl CTEs cuts the full-history
-- traces scans from 4 (2x erc721 + 2x erc1155) to 1.
), address_universe AS (
    -- Every deployed contract, keyed by its own bytecode_hash.
    -- Represents the "not a clone" branch (is_clone = false, implementation_address = own address).
    SELECT
        d.creation_time,
        d.address,
        d.bytecode_hash,
        false AS is_clone,
        d.block_date,
        d.block_month,
        d.address AS implementation_address
    FROM deploys d
    UNION ALL
    -- Every clone, joined to the deploy it references, keyed by the deploy's bytecode_hash.
    -- Represents the "clone of a known deploy" branch (is_clone = true).
    SELECT
        c.creation_time,
        c.address,
        d.bytecode_hash,
        true AS is_clone,
        c.block_date,
        c.block_month,
        c.implementation_address
    FROM clones c
    INNER JOIN deploys d ON c.implementation_address = d.address
), deployed_impls AS (
    SELECT
        au.creation_time,
        au.address,
        au.is_clone,
        au.block_date,
        au.block_month,
        au.implementation_address,
        i.is_creator_token,
        t.token_type
    FROM address_universe au
    INNER JOIN (
        -- Single scan of `traces` matching both erc721 and erc1155
        -- implementation-registration patterns.
        SELECT
            output AS bytecode_hash,
            varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000a9fc664e) > 0 AS is_creator_token,
            (varbinary_position(input, 0x0000000000000000000000000000000000000000000000000000000023b872dd) > 0
             AND varbinary_position(input, 0x0000000000000000000000000000000000000000000000000000000042842e0e) > 0
             AND varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000b88d4fde) > 0) AS is_erc721,
            (varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000f242432a) > 0
             AND varbinary_position(input, 0x000000000000000000000000000000000000000000000000000000002eb2c2d6) > 0) AS is_erc1155
        FROM {{ source(blockchain, 'traces') }}
        WHERE
            to = 0x000000000000000000000000000000000000800e
            AND varbinary_position(input, 0xf5e69a47) = 1
            AND (
                (varbinary_position(input, 0x0000000000000000000000000000000000000000000000000000000023b872dd) > 0
                 AND varbinary_position(input, 0x0000000000000000000000000000000000000000000000000000000042842e0e) > 0
                 AND varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000b88d4fde) > 0)
                OR
                (varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000f242432a) > 0
                 AND varbinary_position(input, 0x000000000000000000000000000000000000000000000000000000002eb2c2d6) > 0)
            )
    ) i ON au.bytecode_hash = i.bytecode_hash
    -- Fan out to per-standard rows. Preserves dual-standard-contract semantics:
    -- a bytecode_hash matching both pattern sets emits both 'erc721' and 'erc1155'.
    CROSS JOIN UNNEST(
        filter(ARRAY[
            IF(i.is_erc721, 'erc721'),
            IF(i.is_erc1155, 'erc1155')
        ], x -> x IS NOT NULL)
    ) AS t(token_type)
)

-- Ensure output table is limited to one record per address
SELECT 
    '{{blockchain}}' as blockchain, 
    min_by(is_creator_token, creation_time) as is_creator_token,
    min_by(token_type, creation_time) as token_type,
    min(creation_time) as creation_time,
    address,
    min_by(is_clone, creation_time) as is_clone,
    min(block_date) as block_date,
    min(block_month) as block_month
FROM
    (
        -- SELECT DISTINCT matches the original's `UNION` (implicit dedup)
        -- between labelled_contracts_721 and labelled_contracts_1155.
        SELECT DISTINCT
            is_creator_token,
            token_type,
            creation_time,
            address,
            is_clone,
            block_date,
            block_month,
            implementation_address
        FROM deployed_impls
    )
WHERE address IS NOT NULL
GROUP BY address

{% endmacro %}
