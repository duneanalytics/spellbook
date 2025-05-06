{# check that all assets exist in the ton.prices_daily table #}
SELECT asset_id, asset_name FROM {{ ref('evaa_ton_assets') }}
LEFT JOIN {{ ref('ton_jetton_price_daily') }} ON token_address = jetton_master
WHERE jetton_master IS NULL
