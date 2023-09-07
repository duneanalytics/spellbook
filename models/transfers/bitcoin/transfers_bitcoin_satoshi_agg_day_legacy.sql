{{ config(
        schema = 'transfers_bitcoin',
        alias = alias('satoshi_agg_day', legacy_model=True),
        tags = ['legacy']
        )
}}

select
    1