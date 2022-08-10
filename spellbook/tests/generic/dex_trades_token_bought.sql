{% test dex_trades_token_bought(model, column_name, dex_trades_seed) %}

with unit_test as
(
    select
        case
            when {{ column_name }} = seed.token_bought_address
                then True
            else False
        end as token_bought_test
    from {{ model }} m
    join {{ dex_trades_seed }} seed
        on m.tx_hash = seed.tx_hash
        and m.block_time = seed.block_time
)
select *
from unit_test
where token_bought_test = True
;

{% endtest %}