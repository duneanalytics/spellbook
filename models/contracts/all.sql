{{  config(
        tags = ['dunesql'], 
        schema = 'contracts',
        alias = alias('all'),
        file_format = 'delta',
        materialized = 'incremental',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'abi_id', 'address', 'created_at']
    )
}}
        
        
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
FROM {{ source('arbitrum', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('avalanche_c', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('base', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('bnb', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('ethereum', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('fantom', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('gnosis', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('optimism', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
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
FROM {{ source('polygon', 'contracts') }}
{% if is_incremental() %}
WHERE created_at >= now() - interval '7' day
{% endif %}
