WITH unit_tests as
(SELECT case when test_data.blockchain=xchange_ethereum_trades.blockchain
              and test_data.project=xchange_ethereum_trades.project
              and test_data.version=xchange_ethereum_trades.version
              and test_data.block_date=xchange_ethereum_trades.block_date
              and test_data.tx_hash=xchange_ethereum_trades.tx_hash
              and test_data.evt_index=xchange_ethereum_trades.evt_index
              and test_data.token_bought_address=xchange_ethereum_trades.token_bought_address
              and test_data.token_bought_amount=xchange_ethereum_trades.token_bought_amount
              and test_data.token_sold_address=xchange_ethereum_trades.token_sold_address
              and test_data.token_sold_amount=xchange_ethereum_trades.token_sold_amount
        then true else false
        end as test
FROM {{ ref('xchange_ethereum_trades') }} xchange_ethereum_trades
JOIN {{ ref('xchange_ethereum_trades_seed') }} test_data ON test_data.tx_hash = xchange_ethereum_trades.tx_hash

)
select count(case when test = false then 1 else null end)/count(*) as pct_mismatch, count(*) as count_rows
from unit_tests
having count(case when test = false then 1 else null end) > count(*)*0.1
