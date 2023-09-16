{{ config(
        schema = 'transfers_evms',
        materialized='view',
        alias = alias('erc20', legacy_model=True),
        tags = ['legacy']
) }}

select
    1