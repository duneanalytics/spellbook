--Run once

--Backfill due to errors caused by Dune delays
--arbitrary old date, should hanlde for previous bugs as well

--chainlink

    --remove bad data
    DELETE FROM chainlink.view_price_feeds
    WHERE hour > '2022-03-16'::date;
    
    --insert new data
    SELECT chainlink.insert_price_feeds(
        '2022-03-16'::date,
        (SELECT max_time FROM ovm2.view_last_updated)
    );
--overall prices

    --remove bad data
    DELETE FROM prices.approx_prices_from_dex_data
    WHERE hour > '2022-03-16'::date;
    --cron should pick up new prices
    
--dex trades
    --remove bad data
    DELETE FROM dex.trades
    WHERE block_time > '2022-03-16'::date;
    --cron should pick up new trades
