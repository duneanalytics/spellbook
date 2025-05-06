{# check that all assets from supply/withdraw/liquidate events are in assets table #}
SELECT 'supply' as table_name, s.asset_id FROM {{ ref('evaa_ton_supply') }} s
LEFT JOIN {{ ref('evaa_ton_assets') }} a ON s.asset_id = a.asset_id
WHERE a.asset_id IS NULL

UNION ALL

SELECT 'withdraw' as table_name, w.asset_id FROM {{ ref('evaa_ton_withdraw') }} w
LEFT JOIN {{ ref('evaa_ton_assets') }} a ON w.asset_id = a.asset_id
WHERE a.asset_id IS NULL

UNION ALL

SELECT 'liquidate(transferred_asset_id)' as table_name, l.transferred_asset_id FROM {{ ref('evaa_ton_liquidate') }} l
LEFT JOIN {{ ref('evaa_ton_assets') }} a ON l.transferred_asset_id = a.asset_id
WHERE a.asset_id IS NULL

UNION ALL

SELECT 'liquidate(collateral_asset_id)' as table_name, l.collateral_asset_id FROM {{ ref('evaa_ton_liquidate') }} l
LEFT JOIN {{ ref('evaa_ton_assets') }} a ON l.collateral_asset_id = a.asset_id
WHERE a.asset_id IS NULL