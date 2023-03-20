{% test assert_evm_address(model, column_name) %}

    with unit_test as
    (
        select
            rlike({{ column_name }}, '0[x][0-9a-f]{40}') as assert_evm_address
        from {{ model }} m
        where blockchain in ('ethereum', 'optimism', 'arbitrum', 'avalanche_c', 'polygon', 'bnb', 'gnosis', 'fantom') -- TODO: turn into generic macro (all_evm_chains fails atm)
    )
    select *
    from unit_test
    where not assert_evm_address

{% endtest %}