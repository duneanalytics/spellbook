WITH unit_tests as
(SELECT
        case when test_data.back_and_forth_trade = wash_trades.filter_2_back_and_forth_trade then True else False end as test1,
        case when test_data.bought_it_three_times_within_a_week = wash_trades.filter_3_bought_or_sold_3x then True else False end as test2,
        case when test_data.funded_by_same_wallet = wash_trades.filter_4_first_funded_by_same_wallet then True else False end as test3,
        case when test_data.is_wash_trade = wash_trades.is_wash_trade then True else False end as test4
FROM {{ ref('nft_wash_trades') }} wash_trades
JOIN {{ ref('nft_wash_trades_refactor_seed') }} test_data ON test_data.unique_trade_id = wash_trades.unique_trade_id

)
select *
from unit_tests
where test1 is false
   or test2 is false
   or test3 is false
   or test4 is false