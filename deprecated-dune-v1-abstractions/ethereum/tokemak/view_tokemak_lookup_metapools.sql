DROP MATERIALIZED VIEW IF EXISTS tokemak.view_tokemak_lookup_metapools
;

CREATE MATERIALIZED VIEW tokemak.view_tokemak_lookup_metapools
(
	tokemak_curve_metapool_id, base_pool_symbol, base_pool_address, pool_token_address, is_active
) AS (
    SELECT 1 as tokemak_curve_metapool_id, 'Curve.fi: DAI/USDC/USDT Pool' as base_pool_symbol, '\xbebc44782c7db0a1a60cb6fe97d0b483032ff1c7'::bytea as base_pool_address, '\x6c3f90f043a72fa612cbac8115ee7e52bde6e490'::bytea as pool_token_address, true
    UNION
    SELECT 2 as tokemak_curve_metapool_id, 'Curve.fi ETH/stETH' as base_pool_symbol, '\xDC24316b9AE028F1497c275EB9192a3Ea0f67022'::bytea as base_pool_address, '\x06325440D014e39736583c165C2963BA99fAf14E'::bytea as pool_token_address, true
    UNION
    SELECT 3 as tokemak_curve_metapool_id, 'Curve.fi FRAX/USDC' as base_pool_symbol, '\xDcEF968d416a41Cdac0ED8702fAC8128A64241A2'::bytea as base_pool_address, '\x3175Df0976dFA876431C2E9eE6Bc45b65d3473CC'::bytea as pool_token_address, true
);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_lookup_metapools (
   tokemak_curve_metapool_id, base_pool_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('1 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_lookup_metapools$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;