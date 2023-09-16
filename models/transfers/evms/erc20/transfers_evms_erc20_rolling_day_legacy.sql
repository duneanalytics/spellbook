{{ config(
        schema = 'transfers_evms',
        alias = alias('erc20_rolling_day', legacy_model=True),
        tags = ['legacy']
        )
}}

select
    1