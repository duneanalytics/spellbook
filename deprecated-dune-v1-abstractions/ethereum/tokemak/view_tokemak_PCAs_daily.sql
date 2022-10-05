CREATE MATERIALIZED VIEW tokemak.view_tokemak_PCAs_daily
(
    "date", asset_symbol, token_address, total_asset_qty, total_liability_qty, total_liability_value_usd,total_liability_value_eth,total_asset_value_usd,total_asset_value_eth, pca_value_usd, pca_value_eth, pca_qty
)
AS (
   WITH liabilities_daily as (
        SELECT "date",symbol, pricing_contract, SUM(total_liability_qty) as total_liability_qty
        FROM tokemak."view_tokemak_outstanding_liabilities_daily" 
        GROUP BY 1,2,3
    ),
    assets_daily as(
        SELECT "date",symbol, token_address,  SUM(total_qty) as total_qty FROM (
            SELECT "date", tl.symbol, a.token_address,tl.pricing_contract, total_qty
            ,tl.is_dollar_stable 
            FROM tokemak."view_tokemak_all_deployable_assets_by_asset_daily" a 
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = a.token_address and is_liability=false 
        UNION
        SELECT "date",tl.symbol, token_address, tl.pricing_contract, tokemak_qty as total_qty
        , tl.is_dollar_stable  
            FROM (
            SELECT  b."date", b.wallet_address, b.token_address, b.source_name, b.symbol, tl.address, b.display_name, b.tokemak_qty 
                FROM tokemak."view_tokemak_wallet_balances_daily" b INNER JOIN 
                tokemak.view_tokemak_lookup_tokens tl on tl.address = b.token_address AND tl.is_pool = false AND tl.symbol <>'TOKE'
                LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" tp on tp."contract_address" = b."token_address" and tp."date" = b."date"
                WHERE tl.is_pool = false and tl.symbol <>'TOKE' 
                ORDER BY source_name, symbol, wallet_address
            ) b
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl on tl.address = b.token_address and is_pool = false and is_liability=false ) as t GROUP BY 1,2,3
    ),

temp_combined as (
    SELECT a."date",a.symbol as asset_symbol
    , a.token_address as token_address
    , a.total_qty as total_asset_qty
    , l.total_liability_qty as total_liability_qty
   , a.total_qty * tp.price_usd as  total_asset_value_usd
    , a.total_qty * tp.price_eth as total_asset_value_eth
    , l.total_liability_qty * tp.price_usd as total_liability_value_usd
    , l.total_liability_qty * tp.price_eth as total_liability_value_eth
    , COALESCE(a.total_qty * tp.price_usd,0) - COALESCE(l.total_liability_qty,0) * tp.price_usd as pca_value_usd
    , COALESCE(a.total_qty * tp.price_eth,0) - COALESCE(l.total_liability_qty,0) * tp.price_eth as pca_value_eth
    FROM assets_daily a 
    LEFT JOIN liabilities_daily l on l.pricing_contract = a.token_address AND l."date" = a."date"
    LEFT JOIN tokemak."view_tokemak_prices_usd_eth_daily" tp on tp."contract_address" = a."token_address" and tp."date" = a."date"
    WHERE NOT (a."date" > '2022-05-10'::date AND (a.token_address = '\x7A75ec20249570c935Ec93403A2B840fBdAC63fd' OR a.token_address='\x482258099de8de2d0bda84215864800ea7e6b03d' OR a.token_address = '\xa693b19d2931d498c5b318df961919bb4aee87a5' OR a.token_address='\xa47c8bf37f92aBed4A126BDA807A7b7498661acD')) --remove the UST tokens
    order by a."date" desc, asset_symbol asc)

SELECT "date", asset_symbol, token_address, total_asset_qty, total_liability_qty, total_liability_value_usd,total_liability_value_eth,total_asset_value_usd,total_asset_value_eth
, pca_value_usd
, pca_value_eth, pca_qty FROM(
    SELECT "date"
    , 'Dollar Stable Coins' as asset_symbol
    ,''::bytea as token_address
    , SUM(total_asset_qty) as total_asset_qty
    , SUM(total_liability_qty) as total_liability_qty
    , SUM(total_liability_value_usd)  as total_liability_value_usd 
    , SUM(total_liability_value_eth)  as total_liability_value_eth
    , SUM(total_asset_value_usd)  as total_asset_value_usd 
    , SUM(total_asset_value_eth)  as total_asset_value_eth
    , SUM(pca_value_usd) as pca_value_usd
    , SUM(pca_value_eth) as pca_value_eth
    , SUM(total_asset_qty-COALESCE(total_liability_qty,0)) as pca_qty
            FROM temp_combined tc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = token_address 
            AND tl.is_dollar_stable = true  AND token_address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' AND pricing_contract <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' and token_address <> '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
            GROUP BY 1,2,3
    UNION
    SELECT "date"
    , 'Ethereum' as asset_symbol
    ,'\xEeeeeEeeeEeEeeEeEeEeeEEEeeeeEeeeeeeeEEeE'::bytea as token_address
    , SUM(total_asset_qty) as total_asset_qty
    , SUM(total_liability_qty) as total_liability_qty
    , SUM(total_liability_value_usd)  as total_liability_value_usd 
    , SUM(total_liability_value_eth)  as total_liability_value_eth
    , SUM(total_asset_value_usd)  as total_asset_value_usd 
    , SUM(total_asset_value_eth)  as total_asset_value_eth
    , SUM(pca_value_usd) as pca_value_usd
    , SUM(pca_value_eth) as pca_value_eth
    , SUM(total_asset_qty-COALESCE(total_liability_qty,0)) as pca_qty
            FROM temp_combined tc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON (tl.address = tc.token_address) AND is_dollar_stable = false
            WHERE (tl.address = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' OR tl.pricing_contract = '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' or tl.address = '\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
            GROUP BY 1,2,3
    UNION
    SELECT "date"
    , asset_symbol
    , token_address 
    , SUM(total_asset_qty) as total_asset_qty
    , SUM(total_liability_qty) as total_liability_qty
    , SUM(total_liability_value_usd)  as total_liability_value_usd 
    , SUM(total_liability_value_eth)  as total_liability_value_eth
    , SUM(total_asset_value_usd)  as total_asset_value_usd 
    , SUM(total_asset_value_eth)  as total_asset_value_eth
    , SUM(pca_value_usd) as pca_value_usd
    , SUM(pca_value_eth) as pca_value_eth
    , SUM(total_asset_qty-COALESCE(total_liability_qty,0)) as pca_qty
            FROM temp_combined tc
            INNER JOIN tokemak."view_tokemak_lookup_tokens" tl ON tl.address = tc.token_address 
            AND is_dollar_stable = false AND tl.address <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' AND tl.pricing_contract <> '\xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2' AND tl.address <>'\xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'
            GROUP BY 1,2,3
    )as t 
    ORDER BY "date" desc, asset_symbol

);
CREATE UNIQUE INDEX ON tokemak.view_tokemak_PCAs_daily (
   "date", token_address
);

-- INSERT INTO cron.job(schedule, command)
-- VALUES ('30 * * * *', $$REFRESH MATERIALIZED VIEW CONCURRENTLY tokemak.view_tokemak_PCAs_daily$$)
-- ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
