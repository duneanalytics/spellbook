{% test perpetual_trades_market_address_legacy(model, column_name, perpetual_trades_seed_legacy) %}

    with unit_test as
    (
        select
            case
                when m.{{ column_name }} = seed.{{ column_name }}
                    then True
                else False
            end as market_address_test
        from {{ model }} m
        inner join {{ perpetual_trades_seed_legacy }} seed
            on m.tx_hash = seed.tx_hash
            and m.block_date = seed.block_date
            and m.blockchain = seed.blockchain
            and m.project = seed.project
            and m.version = seed.version
    )
    select *
    from unit_test
    where market_address_test = False

{% endtest %}
