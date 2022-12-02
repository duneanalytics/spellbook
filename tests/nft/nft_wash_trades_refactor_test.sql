WITH unit_tests AS (

        SELECT
        CASE WHEN test_data.filter_2_back_and_forth_trade = wash_trades.filter_2_back_and_forth_trade THEN true
                ELSE false
                END AS test_1
        , CASE WHEN test_data.filter_3_bought_or_sold_3x = wash_trades.filter_3_bought_or_sold_3x THEN true
                ELSE false
                END AS test_2
        , CASE WHEN test_data.filter_4_first_funded_by_same_wallet = wash_trades.filter_4_first_funded_by_same_wallet THEN true
                ELSE false
                END AS test_3
        , CASE WHEN test_data.is_wash_trade = wash_trades.is_wash_trade THEN true
                ELSE false
                END AS test_4
FROM {{ ref('nft_wash_trades') }} wash_trades
INNER JOIN {{ ref('nft_wash_trades_refactor_seed') }} test_data ON test_data.unique_trade_id = wash_trades.unique_trade_id

)
SELECT *
FROM unit_tests
WHERE test_1 IS false
   OR test_2 IS false
   OR test_3 IS false
   OR test_4 IS false