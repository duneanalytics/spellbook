WITH unit_tests as
(SELECT case when test_data.blockchain=xchange_trades.blockchain
              and test_data.project=xchange_trades.project
              and test_data.version=xchange_trades.version
              and test_data.block_date=xchange_trades.block_date
              and test_data.tx_hash=xchange_trades.tx_hash
              and test_data.evt_index=xchange_trades.evt_index
              and test_data.token_bought_address=xchange_trades.token_bought_address
              and test_data.token_bought_amount=xchange_trades.token_bought_amount
              and test_data.token_sold_address=xchange_trades.token_sold_address
              and test_data.token_sold_amount=xchange_trades.token_sold_amount
        then true else false
        end as test
FROM {{ ref('xchange_trades') }} xchange_trades
JOIN {{ ref('xchange_trades_seed') }} test_data ON test_data.tx_hash = xchange_trades.tx_hash
WHERE project = 'xchange' and xchange_trades.block_date = 2023-06-12
)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1
