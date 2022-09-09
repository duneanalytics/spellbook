{% test compare_column_values_to_seed_values(model, column_name, seed_file_location) %}

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
        join {{ seed_file_location }} seed
            on m.tx_hash = seed.tx_hash
            and m.block_number = seed.block_number
    )

    select test_description
    from unit_test
    where generic_column_test = False

{% endtest %} 
