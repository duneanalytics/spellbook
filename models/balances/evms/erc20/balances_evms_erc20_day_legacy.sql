{{ config(
        schema = 'balances_evms',
        alias = alias('erc20_day', legacy_model=True),
        tags = ['legacy']
        )
}}

select
1