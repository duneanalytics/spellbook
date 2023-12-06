{% test transfers_wallet_token(model, column_name, seed_file_location, agg_grouping) %}
    
        with unit_test as
        (
            select
                case
                    when m.{{ column_name }} = seed.{{ column_name }}
                        then True
                    else False
                end as wallet_token_test
            from {{ model }} m
            inner join {{ seed_file_location }} seed
                on 1 = 1
                and m.blockchain = seed.blockchain
                and m.wallet_address = seed.wallet_address
                and m.token_address = seed.token_address
                and m.{{ agg_grouping }} = seed.{{ agg_grouping }}
        )
        select *
        from unit_test
        where wallet_token_test = False

{% endtest %}