{{ config(
        schema = 'transfers_evms',
        alias = alias('erc20_agg_day', legacy_model=True),
        tags = ['legacy']
        )
}}

select
    1