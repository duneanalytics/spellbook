{{
    config(
        tags = ['legacy'],
        schema = 'balances_celo',
        alias = alias('erc20_hour', legacy_model=True)
    )
}}

-- DUMMY TABLE, WILL BE REMOVED SOON
select 
  1
