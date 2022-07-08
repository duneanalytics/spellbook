CREATE OR REPLACE VIEW balancer_v1.view_transfers_bpt AS
SELECT *
FROM balancer."BPool_evt_Transfer"
UNION ALL
SELECT *
FROM balancer."ConfigurableRightsPool_evt_Transfer"