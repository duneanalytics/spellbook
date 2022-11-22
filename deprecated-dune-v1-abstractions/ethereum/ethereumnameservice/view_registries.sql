CREATE OR REPLACE VIEW ethereumnameservice.view_registries AS
SELECT 
  node,
  label,
  min(evt_block_time) AS min_evt_block_time, 
  max(evt_block_time) AS max_evt_block_time, 
  count(*)  
  FROM (
    SELECT * FROM ethereumnameservice."ENSRegistry_evt_NewOwner"
    UNION
    SELECT * FROM ethereumnameservice."ENSRegistryWithFallback_evt_NewOwner"
  ) r
GROUP BY node, label;
