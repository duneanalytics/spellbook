CREATE OR REPLACE VIEW keep3r_network.view_token_quotes ("timestamp", token, symbol, quote) AS (
		WITH quote_data AS (
			SELECT date_trunc('day', hour) AS timestamp,
				'0x' || encode(
					p0.contract_address,
					'hex'
				) AS token,
				p0.symbol,
				avg(median_price) AS quote
			FROM prices.prices_from_dex_data AS p0
			WHERE p0.symbol IN (
					SELECT symbol
					FROM keep3r_network.token_data
				)
			GROUP BY "timestamp",
				contract_address,
				symbol
		),
		quotes AS (
			(
				SELECT *
				FROM quote_data
			)
			UNION ALL
			(
				SELECT tk_0.timestamp,
					klp.liquidity AS token,
					'kLP-' || token_0 || '/' || token_1 AS symbol,
					2 * sqrt(
						tk_0.quote / tk_1.quote
					) * tk_1.quote AS quote
				FROM keep3r_network.klp_data AS klp
					INNER JOIN quote_data AS tk_0 ON klp.token_0 = tk_0.symbol
					INNER JOIN quote_data AS tk_1 ON klp.token_1 = tk_1.symbol
				WHERE tk_0.timestamp = tk_1.timestamp
			)
		)
		SELECT *
		FROM quotes
	);
