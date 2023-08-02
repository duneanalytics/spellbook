SELECT 
    'arbitrum' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM arbitrum.contracts
UNION ALL
SELECT 
    'avalanche_c' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at
FROM avalanche_c.contracts
UNION ALL
SELECT 
    'base' AS blockchain
    , abi_id
    , abi
    , address
    , "from" 
    , code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM base.contracts
UNION ALL
SELECT
    'bnb' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at
FROM bnb.contracts
UNION ALL
SELECT
    'ethereum' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM ethereum.contracts
UNION ALL
SELECT
    'fantom' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM fantom.contracts
UNION ALL
SELECT 
    'gnosis' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM gnosis.contracts
UNION ALL
SELECT
    'optimism' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM optimism.contracts
UNION ALL
SELECT 
    'polygon' AS blockchain
    , abi_id
    , abi
    , CAST(address AS VARCHAR) AS address
    , CAST("from" AS VARCHAR) AS "from"
    , CAST(code AS VARCHAR) AS code
    , name
    , namespace
    , dynamic
    , base
    , factory
    , detection_source
    , created_at 
FROM polygon.contracts
