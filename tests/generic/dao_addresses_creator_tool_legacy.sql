{% test dao_addresses_creator_tool_legacy(model, column_name, dao_addresses_seed_legacy) %}

    with unit_test as
    (
        select
            case
                when lower(m.{{ column_name }}) = lower(seed.{{ column_name }})
                    then True
                else False
            end as creator_tool_test 
        from {{ model }} m
        inner join {{ dao_addresses_seed_legacy }} seed
            on m.created_date = seed.created_date
            and m.dao = seed.dao
            and m.dao_wallet_address = seed.dao_wallet_address
            and m.blockchain = seed.blockchain
    )
    select *
    from unit_test
    where creator_tool_test  = False

{% endtest %}