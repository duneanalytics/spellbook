{{config(alias='safe_ethereum')}}

SELECT
    array('ethereum') as blockchain,
    address,
    'Gnosis Safe'  || ' version ' || creation_version AS name,
    'safe' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp('2022-10-01') as created_at,
    now() as updated_at
FROM {{ ref('safe_ethereum_safes') }}