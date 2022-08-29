{% test nft_trades_buys_vs_bids(model, column_name, nft_trades_seed) %}

    with unit_test as
    (
        select
            seed.test_description,
            case
                when m.{{ column_name }} = seed.{{ column_name }}
                    then True
                else False
            end as generic_column_test
        from {{ model }} m
        join {{ nft_trades_seed }} seed
            on m.tx_hash = seed.tx_hash
            and m.block_number = seed.block_number
    )

    select test_description
    from unit_test
    where generic_column_test = False

{% endtest %} 
