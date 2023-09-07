{{ config(
        schema = 'transfers_bitcoin',
        alias = alias('satoshi', legacy_model=True),
        tags = ['legacy']
) }}

select
    1