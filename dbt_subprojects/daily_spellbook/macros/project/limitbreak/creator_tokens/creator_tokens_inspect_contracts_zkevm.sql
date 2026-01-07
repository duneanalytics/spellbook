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
), erc721_implementations AS (
    SELECT
        CASE  
        WHEN varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000a9fc664e) > 0 THEN TRUE
        ELSE FALSE
        END AS is_creator_token,
        output AS bytecode_hash
    FROM 
        {{ source(blockchain, 'traces') }}
    WHERE 
        to = 0x000000000000000000000000000000000000800e 
        AND varbinary_position(input, 0xf5e69a47) = 1
        AND varbinary_position(input, 0x0000000000000000000000000000000000000000000000000000000023b872dd) > 0 
        AND varbinary_position(input, 0x0000000000000000000000000000000000000000000000000000000042842e0e) > 0 
        AND varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000b88d4fde) > 0
), erc1155_implementations AS (
    SELECT
        CASE  
        WHEN varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000a9fc664e) > 0 THEN TRUE
        ELSE FALSE
        END AS is_creator_token,
        output AS bytecode_hash
    FROM 
        {{ source(blockchain, 'traces') }}
    WHERE 
        to = 0x000000000000000000000000000000000000800e 
        AND varbinary_position(input, 0xf5e69a47) = 1
        AND varbinary_position(input, 0x00000000000000000000000000000000000000000000000000000000f242432a) > 0 
        AND varbinary_position(input, 0x000000000000000000000000000000000000000000000000000000002eb2c2d6) > 0 
), deployed_erc721_contracts AS (
    SELECT
        d.creation_time,
        d.address,
        d.bytecode_hash,
        d.block_date,
        d.block_month,
        ei.is_creator_token
    FROM
        deploys d inner join 
        erc721_implementations ei on d.bytecode_hash = ei.bytecode_hash
), deployed_erc1155_contracts AS (
    SELECT
        d.creation_time,
        d.address,
        d.bytecode_hash,
        d.block_date,
        d.block_month,
        ei.is_creator_token
    FROM
        deploys d inner join 
        erc1155_implementations ei on d.bytecode_hash = ei.bytecode_hash
), labelled_contracts_721 AS (
    SELECT 
        ed.is_creator_token,
        'erc721' AS token_type,
        c.creation_time,
        c.address,
        true AS is_clone,
        c.block_date,
        c.block_month,
        c.implementation_address
    FROM 
        clones c INNER JOIN 
        deployed_erc721_contracts ed ON c.implementation_address = ed.address
    UNION ALL
    SELECT 
        ed.is_creator_token,
        'erc721' AS token_type,
        ed.creation_time,
        ed.address,
        false AS is_clone,
        ed.block_date,
        ed.block_month,
        ed.address as implementation_address
    FROM deployed_erc721_contracts ed
), labelled_contracts_1155 AS (
    SELECT 
        ed.is_creator_token,
        'erc1155' AS token_type,
        c.creation_time,
        c.address,
        true AS is_clone,
        c.block_date,
        c.block_month,
        c.implementation_address
    FROM 
        clones c INNER JOIN 
        deployed_erc1155_contracts ed ON c.implementation_address = ed.address
    UNION ALL
    SELECT 
        ed.is_creator_token,
        'erc1155' AS token_type,
        ed.creation_time,
        ed.address,
        false AS is_clone,
        ed.block_date,
        ed.block_month,
        ed.address as implementation_address
    FROM deployed_erc1155_contracts ed
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
        SELECT * FROM labelled_contracts_721
        UNION
        SELECT * FROM labelled_contracts_1155
    )
WHERE address IS NOT NULL
GROUP BY address

{% endmacro %}