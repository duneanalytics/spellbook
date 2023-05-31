{{ config(alias='days', tags=['static'], materialized = 'table')}}

select explode(sequence(
       date('2000-01-01')
       ,  date('2100-01-01')
        ,interval 1 day)) as day
order by day asc