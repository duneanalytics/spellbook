{% test daos_addresses_wallet_address(model, column_name, daos_addresses_seed) %}

    with unit_test as
    (
        select
            case
                when lower(m.{{ column_name }}) = lower(seed.{{ column_name }})
                    then True
                else False
            end as wallet_address_test 
        from {{ model }} m
        inner join {{ daos_addresses_seed }} seed
            on m.dao = seed.dao
            and m.created_date = seed.created_date
            and m.blockchain = seed.blockchain
            and m.dao_creator_tool = seed.dao_creator_tool
    )
    select *
    from unit_test
    where wallet_address_test = False

{% endtest %}