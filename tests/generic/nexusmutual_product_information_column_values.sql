{% test nexusmutual_product_information_column_values(model, column_name, nexusmutual_ethereum_product_information_seed) %}

    with unit_test as
    (
        select
            case
                when m.{{ column_name }} = seed.{{ column_name }}
                    then True
                else False
            end as generic_column_test
        from {{ model }} m
        join {{ nexusmutual_ethereum_product_information_seed }} seed
            on m.contract_address = seed.contract_address
            and m.product_name = seed.product_name
            and m.product_type = seed.product_type
            and m.date_added = seed.date_added
            and m.syndicate = seed.syndicate
    )

    select *
    from unit_test
    where generic_column_test = False

{% endtest %} 