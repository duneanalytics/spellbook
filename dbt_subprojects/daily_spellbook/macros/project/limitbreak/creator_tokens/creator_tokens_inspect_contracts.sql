{% macro creator_tokens_inspect_contracts(blockchain) %}

WITH clones AS (
    SELECT 
        true as is_clone, 
        block_time as creation_time, 
        address, 
        varbinary_substring(code, varbinary_position(code, 0x363d3d373d3d3d363d73) + 10, 20) as implementation_address, 
        code,
        CAST(date_trunc('day', block_time) as date)  as block_date,
        CAST(date_trunc('month', block_time) as date)  as block_month
    FROM 
        {{ source(blockchain, 'traces') }}
    WHERE 
        varbinary_length(code) > 0 
        AND block_time > TIMESTAMP '2023-05-01' 
        AND varbinary_position(code, 0x363d3d373d3d3d363d73) > 0
        {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
        {% endif %}
), deploys AS (
    SELECT 
        false as is_clone, 
        block_time as creation_time, 
        address, 
        0x0000000000000000000000000000000000000000 as implementation_address, 
        code, 
        code as implementation_code,
        CAST(date_trunc('day', block_time) as date)  as block_date,
        CAST(date_trunc('month', block_time) as date)  as block_month
    FROM 
        {{ source(blockchain, 'traces') }}
    WHERE 
        varbinary_length(code) > 0 
        AND block_time > TIMESTAMP '2023-05-01' 
        AND varbinary_position(code, 0x363d3d373d3d3d363d73) = 0
), clones_with_implementation_code AS (
    SELECT 
        t1.is_clone, 
        t1.creation_time, 
        t1.address, 
        t1.implementation_address, 
        t2.code as implementation_code,
        t1.block_date,
        t1.block_month
    FROM 
        clones t1 
        INNER JOIN deploys t2 ON t1.creation_time >= t2.creation_time and t2.address = t1.implementation_address
), deploys_with_implementation_code AS (
    SELECT 
        is_clone, 
        creation_time, 
        address, 
        implementation_address, 
        code as implementation_code,
        block_date,
        block_month
    FROM 
        deploys
    {% if is_incremental() %}
    WHERE {{incremental_predicate('creation_time')}}
    {% endif %}
), contracts AS (
    SELECT * 
    FROM clones_with_implementation_code 
    UNION 
    SELECT * 
    FROM deploys_with_implementation_code
), labelled_contracts_721 AS (
    SELECT 
        CASE WHEN varbinary_position(implementation_code, 0x8063a9fc664e14) > 0 THEN true ELSE false END AS is_creator_token, 
        'erc721' as token_type, 
        creation_time, 
        address, 
        is_clone,
        block_date,
        block_month
    FROM 
        contracts
    WHERE 
        varbinary_position(implementation_code, 0x806323b872dd14) > 0 
        AND varbinary_position(implementation_code, 0x806342842e0e14) > 0 
        AND varbinary_position(implementation_code, 0x8063b88d4fde14) > 0
), labelled_contracts_1155 AS (
    SELECT 
        CASE WHEN varbinary_position(implementation_code, 0x8063a9fc664e14) > 0 THEN true ELSE false END AS is_creator_token, 
        'erc1155' as token_type, 
        creation_time, 
        address, 
        is_clone,
        block_date,
        block_month
    FROM 
        contracts
    WHERE 
        varbinary_position(implementation_code, 0x8063f242432a14) > 0 
        AND varbinary_position(implementation_code, 0x80632eb2c2d614) > 0
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
