{{config(
     alias = 'op_trades_derived_archetype'
)}}


WITH combined_trades AS
(SELECT address, 
(CASE
WHEN (nft_traders_label = 'Elite NFT Trader' AND dex_trades_label = 'Elite DEX Trader'
AND perp_trades_label = 'Elite Perp Trader') THEN 'Master Trader'
ELSE 'Jack of All Trades'
END) AS label
FROM
(SELECT nft.address, nft.name AS nft_traders_label, 
dex.name AS dex_trades_label, perp.name AS perp_trades_label
FROM {{ ref('labels_op_nft_traders') }} nft
JOIN {{ ref('labels_op_dex_traders') }} dex
ON nft.address = dex.address
JOIN {{ ref('labels_op_perpetual_traders') }} perp
ON nft.address = perp.address
)
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_traders' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-06' AS created_at,
    NOW() AS updated_at,
    'op_trades_derived_archetype' AS model_name,
    'persona' AS label_type
FROM
    combined_trades