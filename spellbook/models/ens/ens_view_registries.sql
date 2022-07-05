{{config(alias='view_registries')}}
SELECT 
  node,
  label,
  min(evt_block_time) AS min_evt_block_time, 
  max(evt_block_time) AS max_evt_block_time, 
  count(*)  
  FROM (
    SELECT * 
    FROM {{source('ethereumnameservice_ethereum', 'ENSRegistry_evt_NewOwner')}}
    UNION
    SELECT * FROM {{source('ethereumnameservice_ethereum', 'ENSRegistryWithFallback_evt_NewOwner')}}
  ) r
GROUP BY node, label;
