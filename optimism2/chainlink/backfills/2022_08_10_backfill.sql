--Backfill due to errors caused by Dune delays
--arbitrary old date, should hanlde for previous bugs as well

--chainlink
SELECT chainlink.insert_price_feeds(
    '2022-03-16'::date,
    (SELECT max_time FROM ovm2.view_last_updated)
)
;
--overall prices
SELECT prices.insert_approx_prices_from_dex_data(
        '2022-03-16'::date,
        (SELECT max_time FROM ovm2.view_last_updated)
    );

