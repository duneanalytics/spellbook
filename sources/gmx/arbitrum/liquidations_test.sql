-- Test to ensure liquidation events are being captured
SELECT
    COUNT(*) as liquidation_count,
    COUNT(DISTINCT liquidated_account) as unique_accounts,
    AVG(position_size_usd) as avg_position_size,
    AVG(collateral_usd) as avg_collateral,
    MIN(evt_block_time) as earliest_event,
    MAX(evt_block_time) as latest_event
FROM
    gmx_arbitrum.liquidations
WHERE
    evt_block_time >= NOW() - INTERVAL '7 days'
HAVING 
    COUNT(*) > 0 -- Ensure we have some data
    AND COUNT(DISTINCT liquidated_account) > 0 -- Ensure multiple accounts are affected
    AND AVG(position_size_usd) > 0 -- Ensure positions have value
    AND AVG(collateral_usd) > 0; -- Ensure collateral has value 
