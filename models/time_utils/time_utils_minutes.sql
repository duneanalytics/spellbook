{{ config( alias='minutes', tags=['static'], materialized = 'table')}}

select explode(sequence(
         date_trunc('minute', now() - interval '20 year')
        ,date_trunc('minute', now() + interval '20 year')
        ,interval 1 day)) as minute
order by minute asc