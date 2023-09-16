{{ config(
        schema = 'transfers_evms',
        alias = alias('erc20', legacy_model=True),
        tags = ['legacy']
) }}

select
    1