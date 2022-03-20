SELECT prices.insert_approx_prices_from_dex_data(
        	'02-01-2022'::date,
        	now()
    	)
	WHERE EXISTS (SELECT 1 FROM prices.approx_prices_from_dex_data
		WHERE hour < '02-15-2022'::date AND symbol = 'TCAP'
		AND median_price > 1000000
		);
