{{config(
        materialized = 'table',
        tags=['static'],
        schema='labels',
        alias = 'bridges_near_native',)}}

SELECT blockchain, address as address, name, category, contributor, source, created_at, updated_at, model_name, label_type
FROM (VALUES
  ('near', 'adb098304d446a7fdf3760a3678fefbe95301d85c952c111c9439fcd26e7c939', 'rainbow bridge 1', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'aurora', 'aurora', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'aurora.pool.near', 'aurora bridge 1', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'aurora.near', 'aurora bridge 2', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'client.bridge.near', 'rainbow bridge 2', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'contract.portalbridge.near', 'wormhole bridge 1', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'contract.wormhole_crypto.near', 'wormhole', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'multichainorg.near', 'multichain', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier'),
  ('near', 'relay.aurora', 'aurora relay', 'bridge', 'Sector920', 'static', DATE '2025-05-20', now(), 'bridges_near', 'identifier')
) AS x ( blockchain, address, name, category, contributor, source, created_at, updated_at, model_name, label_type)