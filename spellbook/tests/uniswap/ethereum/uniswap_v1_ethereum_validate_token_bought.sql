with unit_test as
(
    select
        case
            when (uni_v1.token_bought_address = test_data.token_bought_address and uni_v1.token_sold_address = test_data.token_sold_address)
            then True
            else False
        end as token_bought_test
    from {{ ref('uniswap_v1_ethereum_trades') }} uni_v1
    join {{ ref('dex_trades_seed') }} test_data
        on uni_v1.tx_hash = test_data.tx_hash
        and uni_v1.block_time = test_data.block_time
)
select *
from unit_test
where token_bought_test = True