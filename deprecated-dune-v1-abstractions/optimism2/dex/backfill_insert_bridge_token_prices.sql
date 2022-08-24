CREATE OR REPLACE FUNCTION dex.backfill_insert_bridge_token_prices(start_ts timestamptz, end_ts timestamptz=now()) RETURNS integer
LANGUAGE plpgsql AS $function$
DECLARE r integer;
BEGIN
WITH rows AS (
    INSERT INTO prices.approx_prices_from_dex_data (
        contract_address,
        hour,
        median_price,
        sample_size,
        symbol,
        decimals
    )
    
 WITH dex_price_bridge_tokens AS (
            SELECT DATE_TRUNC('hour', pr.hour) AS hour, "bridge_token" AS token, "bridge_symbol" AS symbol, "bridge_decimals" AS decimals, median_price * price_ratio AS median_price, pr.sample_size,
            DENSE_RANK() OVER (PARTITION BY bridge_token ORDER BY pr.hour DESC) AS hrank

            FROM prices.hourly_bridge_token_price_ratios pr

            INNER JOIN prices.approx_prices_from_dex_data p
                    ON pr.erc20_token = p.contract_address
                    AND DATE_TRUNC('hour',pr.hour) = p.hour

            WHERE pr.hour >= start_ts
            AND pr.hour < end_ts
        )
    
    SELECT token AS contract_address, hour, median_price, sample_size, symbol, decimals
    FROM dex_price_bridge_tokens
    
    ON CONFLICT (contract_address, hour) DO UPDATE SET median_price = EXCLUDED.median_price, sample_size = EXCLUDED.sample_size
    RETURNING 1
)
SELECT count(*) INTO r from rows;
RETURN r;
END
$function$;
