{{
    config(
        tags = ['legacy'],
        schema = 'balances_celo',
        alias = alias('erc1155_latest', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 1
