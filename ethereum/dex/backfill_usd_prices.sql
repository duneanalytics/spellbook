CREATE OR REPLACE FUNCTION dex.backfill_usd_amount(start_ts timestamptz, end_ts timestamptz=now()) RETURNS boolean
LANGUAGE plpgsql AS $function$
BEGIN

-- Sometimes the prices table is updated with a new token and tables like dex.trades
-- are not backfilled.
-- This query will find those missing prices and fill them.
UPDATE
	dex.trades
SET
	usd_amount = new_prices.usd_amount
FROM
	(
		SELECT
			d.project,
			d.tx_hash,
			d.trace_address,
			d.evt_index,
			d.trade_id,
			d.usd_amount as original,
			d.token_a_amount_raw / 10 ^ (CASE token_a_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pa.decimals END) * (CASE token_a_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN pe.price ELSE pa.price END) as pa_price,
			d.token_b_amount_raw / 10 ^ (CASE token_b_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pb.decimals END) * (CASE token_b_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN pe.price ELSE pb.price END) as pb_price,
			d.token_a_symbol as pa_symb,
			d.token_b_symbol as pb_symb,
			coalesce(
				d.token_a_amount_raw / 10 ^ (CASE token_a_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pa.decimals END) * (CASE token_a_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN pe.price ELSE pa.price END),
				d.token_b_amount_raw / 10 ^ (CASE token_b_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN 18 ELSE pb.decimals END) * (CASE token_b_address WHEN '\xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN pe.price ELSE pb.price END)
			) as usd_amount
		FROM
			dex.trades d
			left join prices.usd pa on d.token_a_address = pa.contract_address
			AND pa."minute" >= start_ts
			AND pa."minute" < end_ts
			AND date_trunc('minute', d.block_time) = pa."minute"
			left join prices.usd pb on d.token_b_address = pb.contract_address
			AND pb."minute" >= start_ts
			AND pb."minute" < end_ts
			AND date_trunc('minute', d.block_time) = pb."minute"
			LEFT JOIN prices.layer1_usd pe ON pe.minute = date_trunc('minute', d.block_time)
			AND pe.symbol = 'ETH'
			AND pe.minute >= start_ts
			AND pe.minute < end_ts
		WHERE
			block_time >= start_ts
			AND block_time < end_ts
			and d.usd_amount is NULL
			and (pa.price is not NULL or pb.price is not null or pe.price is not null)
	) as new_prices
where
	block_time >= start_ts
	AND block_time < end_ts
	AND trades.project = new_prices.project
	AND trades.tx_hash = new_prices.tx_hash
	-- These coalesces are to handle the times these values are null
	-- because in postgres NULL = NULL equals NULL :face_palm:
	AND COALESCE(trades.trace_address, '{-1}') = COALESCE(new_prices.trace_address, '{-1}')
	AND COALESCE(trades.evt_index, -1) = COALESCE(new_prices.evt_index, -1)
	AND trades.trade_id = new_prices.trade_id;

RETURN TRUE;
END
$function$;

-- daily backfill run
-- back fill last 3 days in case something goes wrong over the weekend
INSERT INTO cron.job (schedule, command)
VALUES ('52 * * * *', $$
    SELECT dex.backfill_usd_amount(
        (SELECT now() - interval '3 days'),
        (SELECT now() - interval '20 minutes'));
$$)
ON CONFLICT (command) DO UPDATE SET schedule=EXCLUDED.schedule;
