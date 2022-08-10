--Backfill due to errors caused by Dune delays
SELECT chainlink.insert_price_feeds(
    '2022-03-16'::date,
    (SELECT MAX(block_time) FROM optimism.transactions)
)
;
