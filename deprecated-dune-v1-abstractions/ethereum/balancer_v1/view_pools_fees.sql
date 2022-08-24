CREATE OR REPLACE VIEW balancer_v1.view_pools_fees AS
SELECT *
FROM balancer."BPool_call_setSwapFee"
WHERE call_success
UNION ALL
SELECT *
FROM balancer."ConfigurableRightsPool_call_setSwapFee"
WHERE call_success