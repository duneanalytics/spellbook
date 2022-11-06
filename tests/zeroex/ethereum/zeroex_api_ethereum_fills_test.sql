{% test zeroex_api_ethereum_fills_test(model, column_name, zeroex_api_ethereum_fills_sample) %}


WITH unit_tests AS
(SELECT CASE WHEN fills.taker_token_amount = fills_sample.taker_token_amount 
    AND fills.maker_token_amount = fills_sample.maker_token_amount
    AND fills.volume_usd = fills_sample.volume_usd
    THEN True ELSE False END AS amount_test
FROM {{ ref('zeroex_api_ethereum_fills') }} fills
    JOIN {{ ref('zeroex_api_ethereum_fills_sample') }} fills_sample 
    ON fills.tx_hash = fills_sample.tx_hash
    AND fills.evt_index = fills_sample.evt_index

)
SELECT count(CASE WHEN amount_test = false THEN 1 ELSE NULL END)/count(*) AS pct_mismatch, count(*) AS COUNT_ROWS
FROM unit_tests
HAVING count(CASE WHEN amount_test = false THEN 1 ELSE NULL END) > count(*)*0.05


{% endtest %}