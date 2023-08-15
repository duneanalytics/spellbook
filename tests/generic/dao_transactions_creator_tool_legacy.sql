{% test dao_transactions_creator_tool_legacy(model, column_name, dao_transactions_seed_legacy) %}

    with unit_test as
    (
        select
            case
                when lower(m.{{ column_name }}) = lower(seed.{{ column_name }})
                    then True
                else False
            end as creator_tool_test 
        from {{ model }} m
        inner join {{ dao_transactions_seed_legacy }} seed
            on m.tx_hash = seed.tx_hash
            and m.block_date = seed.block_date
            and m.dao = seed.dao 
            and m.dao_wallet_address = seed.dao_wallet_address
            and m.tx_type = seed.tx_type
            and m.blockchain = seed.blockchain
            and m.value = seed.value 
    )
    select *
    from unit_test
    where creator_tool_test = False

{% endtest %}