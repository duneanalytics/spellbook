{{ config(
        schema = 'transfers_bitcoin',
        alias = alias('satoshi_rolling_day', legacy_model=True),
        tags = ['legacy']
        )
}}

select
    1