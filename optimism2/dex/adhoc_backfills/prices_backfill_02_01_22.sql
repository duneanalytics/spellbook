--fix chainlink feeds - which stall after mid-Feb
SELECT chainlink.insert_price_feeds(
        	'01-01-2021'::date,
        	now()
    	)
	WHERE EXISTS (SELECT 1 FROM prices.approx_prices_from_dex_data
		WHERE DATE_TRUNC('day',hour) = '03-07-2022'::date AND symbol = 'sETH'
		AND median_price > 3000
		);

		
SELECT prices.insert_approx_prices_from_dex_data(
        	'01-01-2022'::date,
        	now()
    	)
	WHERE EXISTS (SELECT 1 FROM prices.approx_prices_from_dex_data
		WHERE hour < '02-15-2022'::date AND symbol = 'TCAP'
		AND median_price > 1000000
		);
