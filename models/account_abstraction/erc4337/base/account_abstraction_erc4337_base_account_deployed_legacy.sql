{{ config(
    alias = alias('account_deployed', legacy_model=True),
    tags=['legacy']
)}}

select 1 as unique_id
