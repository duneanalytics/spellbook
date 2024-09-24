{{ config(
        schema = 'balancer',
        alias = 'view_pools_latest', 
        post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                spell_type = "project",
                                spell_name = "balancer",
                                contributors = \'["viniabussafi"]\') }}'
        )
}}

SELECT
    m.blockchain,
    m.pool_id,
    m.pool_address,
    m.pool_symbol,
    m.pool_type,
    m.factory_version,
    m.factory_address,
    m.creation_date,
    SUM(CASE WHEN block_date = (SELECT MAX(block_date) FROM {{ref('balancer_pools_metrics_daily')}})
    THEN tvl_usd END) AS tvl_usd,
    SUM(CASE WHEN block_date = (SELECT MAX(block_date) FROM {{ref('balancer_pools_metrics_daily')}})
    THEN tvl_eth END) AS tvl_eth,
    SUM(swap_amount_usd) AS swap_volume,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '365' day THEN swap_amount_usd END) AS swap_volume_1y,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '30' day THEN swap_amount_usd END) AS swap_volume_30d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '7' day THEN swap_amount_usd END) AS swap_volume_7d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '1' day THEN swap_amount_usd END) AS swap_volume_1d,  
    SUM(fee_amount_usd) AS fees_collected,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '365' day THEN fee_amount_usd END) AS fees_collected_1y,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '30' day THEN fee_amount_usd END) AS fees_collected_30d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '7' day THEN fee_amount_usd END) AS fees_collected_7d,
    SUM(CASE WHEN block_date > CURRENT_DATE - INTERVAL '1' day THEN fee_amount_usd END) AS fees_collected_1d  
FROM {{source('balancer', 'factory_pool_mapping')}} m
LEFT JOIN {{ref('balancer_pools_metrics_daily')}} p ON m.blockchain = p.blockchain
AND m.pool_address = p.project_contract_address
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8