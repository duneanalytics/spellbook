CREATE OR REPLACE FUNCTION nft.backfill_usd_amount() RETURNS boolean
LANGUAGE plpgsql AS $function$
BEGIN

-- original author: @va3093 https://github.com/duneanalytics/abstractions/pull/422/files
-- Sometimes the prices table is updated with a new token and tables like nft.trades
-- are not backfilled.
-- This query will find those missing prices and fill them.
UPDATE
	nft.trades
SET
	usd_amount = new_prices.new_price
FROM
	(
		SELECT
			n.platform,
			n.tx_hash,
			n.trace_address,
			n.evt_index,
			n.trade_id,
			n.original_amount * p.price as new_price
		FROM
			nft.trades n
			LEFT JOIN prices.usd p ON p.contract_address = n.currency_contract
			AND date_trunc('minute', n.block_time) = p."minute"
		WHERE
			usd_amount IS NULL
			AND p.symbol IS NOT NULL
	) as new_prices
where
	trades.platform = new_prices.platform
	AND trades.tx_hash = new_prices.tx_hash
	-- These coalesces are to handle the times these values are null
	-- because in postgres NULL = NULL equals NULL :face_palm:
	AND COALESCE(trades.trace_address, '{-1}') = COALESCE(new_prices.trace_address, '{-1}')
	AND COALESCE(trades.evt_index, -1) = COALESCE(new_prices.evt_index, -1)
	AND trades.trade_id = new_prices.trade_id;

RETURN TRUE;
END
$function$;

-- historical fill
SELECT nft.backfill_usd_amount();

INSERT INTO cron.job (schedule, command)
VALUES ('12 12 * * *', $$
    SELECT nft.backfill_usd_amount();
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
