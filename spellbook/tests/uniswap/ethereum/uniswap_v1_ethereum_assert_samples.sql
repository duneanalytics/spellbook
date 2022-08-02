/*
Bootstrapped validation test against legacy Postgres values using seed CSV file
Postgres query:
    select
        block_time
        , tx_hash
        , token_a_amount
        , token_b_amount
    from
        dex.trades
    where
        project = 'Uniswap'
        and version = '1'
        and (
            block_time between '2022-07-01' and '2022-08-01'
        )
    order by
        block_time desc
    limit
        1000;

Also manually check etherscan info for the first 5 rows
*/

with unit_tests as
(
    select
        case
            when test_data.token_a_amount = us_trades.token_a_amount
                and test_data.token_b_amount = us_trades.token_b_amount
            then True
            else False
        end as price_test
    from {{ ref('uniswap_trades') }} us_trades
    join {{ ref('uniswap_v1_ethereum_trades_postgres') }} test_data
        on test_data.tx_hash = us_trades.tx_hash
    where us_trades.blockchain = 'ethereum' and us_trades.version = 'v1'
)
select
    count(
        case
            when price_test = False 
            then 1
            else null
        end
        ) / count(*) as pct_mismatch
    , count(*) as count_rows
from unit_tests
having
    count(
        case
            when price_test = False
            then 1
            else null
        end
        ) > count(*) * 0.05