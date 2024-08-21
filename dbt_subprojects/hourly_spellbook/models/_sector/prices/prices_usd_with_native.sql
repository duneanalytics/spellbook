{{ config(
        schema='prices',
        alias = 'usd_with_native'
        )
}}
-- this is a TEMPORARY spell that should be incorporated in the general prices models.
-- more discussion here: https://github.com/duneanalytics/spellbook/issues/6577

select * from {{ref('prices_usd')}}
union all
select * from {{ref('prices_usd_native')}}
