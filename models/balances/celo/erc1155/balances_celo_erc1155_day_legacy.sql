{{
    config(
        tags = ['legacy'],
        schema = 'balances_celo',
        alias = alias('erc1155_day', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
