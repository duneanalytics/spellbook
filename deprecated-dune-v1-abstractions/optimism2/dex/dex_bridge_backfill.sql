-- one-off to fix bridge token prices after initial insert bug.

SELECT dex.backfill_insert_bridge_token_prices(
    '2021-11-11',
    now()
)
WHERE EXISTS (
  SELECT median_price FROM
  prices."approx_prices_from_dex_data"
  WHERE symbol = 'hETH'
  AND hour = '03-02-2022'::date
  AND median_price > 4500 --we know this is wrong
);
