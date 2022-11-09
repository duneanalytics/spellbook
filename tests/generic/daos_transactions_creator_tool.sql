{% test daos_transactions_creator_tool(model, column_name, daos_transactions_seed) %}

    with unit_test as
    (
        select
            case
                when lower(m.{{ column_name }}) = lower(seed.{{ column_name }})
                    then True
                else False
            end as creator_tool_test 
        from {{ model }} m
        inner join {{ daos_transactions_seed }} seed
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