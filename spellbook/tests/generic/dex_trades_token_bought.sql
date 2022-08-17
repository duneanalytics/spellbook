{% test dex_trades_token_bought(model, column_name, dex_trades_seed) %}

    with unit_test as
    (
        select
            case
                when m.{{ column_name }} = seed.{{ column_name }}
                    then True
                else False
            end as token_bought_test
        from {{ model }} m
        inner join {{ dex_trades_seed }} seed
            on m.tx_hash = seed.tx_hash
            and m.evt_index = seed.evt_index
            and m.block_date = seed.block_date
            and m.blockchain = seed.blockchain
            and m.project = seed.project
            and m.version = seed.version
    )
    select *
    from unit_test
    where token_bought_test = False

{% endtest %}