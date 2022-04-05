WITH 

-- Only last Reverse Registrar transaction per eth_addr
ens_transactions AS (
    SELECT DISTINCT ON ("from") "from" AS eth_addr, block_number, block_time, hash as tx_hash
    FROM ethereum."transactions" 
    WHERE
        -- Old Reverse Registrar and Reverse Registrar contracts
        ("to" = '\x9062c0a6dbd6108336bcbe4593a3d1ce05512069' OR "to" = '\x084b1c3c81545d370f3634392de611caabff8148')
         -- Only successful transactions
        AND success IS TRUE
        AND block_time >= '{{timestamp}}'
    ORDER BY "from", block_number DESC
),

-- Old Reverse Registrar
ens_calls_v1 AS (
    SELECT DISTINCT ON (name) name AS ens_name, call_block_number AS block_number, call_tx_hash AS tx_hash
    FROM ethereumnameservice."ReverseRegistrar_v1_call_setName"
    -- To avoid issues with long names on Dune's side
    WHERE length(name) < 10000 AND call_success IS TRUE
    ORDER BY ens_name, block_number DESC
),

-- New Reverse Registrar
ens_calls_v2 AS (
    SELECT DISTINCT ON (name) name AS ens_name, call_block_number AS block_number, call_tx_hash AS tx_hash
    FROM ethereumnameservice."ReverseRegistrar_v2_call_setName"
    -- To avoid issues with long names on Dune's side
    WHERE length(name) < 10000 AND call_success IS TRUE
    ORDER BY ens_name, block_number DESC
),

-- Reverse Registrar setName calls
ens_calls AS (
    SELECT * FROM ens_calls_v1
    UNION
    SELECT * FROM ens_calls_v2
)

-- Latest snapshot of ENS Reverse Records
SELECT 
    t.eth_addr AS address, 
    LOWER(c.ens_name) AS label,
    'ens name reverse' AS type,
    'zxsasha' AS author
FROM ens_transactions AS t
INNER JOIN ens_calls AS c ON c.block_number = t.block_number AND c.tx_hash = t.tx_hash AND c.ens_name <> '0x0000000000000000000000000000000000000000'
-- Filter all possible Unicode Whitespace characters to prevent malicious issues in the future
WHERE LOWER(c.ens_name) !~ '[\u0009\u000A\u000B\u000C\u000D\u0020\u0085\u00A0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200A\u202f\u205f\u3000]';
