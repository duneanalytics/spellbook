{{ config(
        schema = 'balances_bitcoin',
        alias = alias('satoshi_day', legacy_model=True),
        tags = ['legacy']
        )
}}

select
1