{{ config(alias='days', tags=['static'], materialized = 'table')}}

select explode(sequence(
        date_trunc('day', now() - interval '20 year')
        ,date_trunc('day', now() + interval '20 year')
        ,interval 1 day)) as day
order by day asc